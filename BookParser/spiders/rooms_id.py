import os
import scrapy
import mysql.connector
from tabulate import tabulate
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from typing import List, Tuple, Dict, Optional
from contextlib import contextmanager


class RoomsIDSpider(scrapy.Spider):
    name = "rooms_id"
    allowed_domains = ["www.booking.com"]
    start_urls = ["https://www.booking.com"]

    def __init__(self, mode=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = mode
        self.today = datetime.now().date()
        self.checkin, self.checkout = self.get_monthly_week_dates()
        self.room_data: List[Dict] = []
        self.connection = None
        self.cursor = None
        
    @contextmanager
    def database_connection(self):
        """Context manager for database connections"""
        config = {
            'user': os.getenv('DATABASE_USER'),
            'password': os.getenv('DATABASE_PASSWORD'),
            'host': os.getenv('DATABASE_HOST'),
            'database': os.getenv('DATABASE_NAME'),
            'raise_on_warnings': True
        }
        
        try:
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor(dictionary=True)
            yield connection, cursor
        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()

    def format_link(self, link: str, checkin: datetime.date, checkout: datetime.date) -> str:
        """Format booking.com URL with updated parameters"""
        url = urlparse(link)
        query_parameters = parse_qs(url.query)
        
        query_parameters.update({
            'group_adults': ['1'],
            'no_rooms': ['1'],
            'group_children': ['0'],
            'checkin': [checkin.strftime('%Y-%m-%d')],
            'checkout': [checkout.strftime('%Y-%m-%d')],
            'selected_currency': ['USD']
        })
        
        return urlunparse(url._replace(query=urlencode(query_parameters, doseq=True)))

    def get_monthly_week_dates(self, num_months: int = 3) -> Tuple[List[datetime.date], List[datetime.date]]:
        """Generate check-in and check-out dates for the specified number of months"""
        today = datetime.now().date()
        current_week_of_month = (today.day - 1) // 7 + 1
        checkin, checkout = [], []

        for month in range(num_months):
            next_month = today.replace(day=1) + timedelta(days=32 * (month + 1))
            next_month = next_month.replace(day=1)
            
            target_date = next_month + timedelta(days=(current_week_of_month - 1) * 7)
            
            if target_date.month != next_month.month:
                target_date = next_month + timedelta(days=(current_week_of_month - 2) * 7)
            
            while target_date.weekday() != 0:
                target_date -= timedelta(days=1)
            
            checkin.extend([target_date + timedelta(days=day) for day in range(7)])
        
        checkout = [date + timedelta(days=1) for date in checkin]
        return checkin, checkout

    def write_to_db(self, booking_id: int):
        """Write room data to database with improved active status handling"""
        print('\nWRITE TO DB\n')
        
        if not self.room_data:
            # If no rooms found, mark all rooms for this booking_id as inactive
            with self.database_connection() as (connection, cursor):
                cursor.execute(
                    "UPDATE rooms_id SET active = FALSE WHERE booking_id = %s",
                    (booking_id,)
                )
                connection.commit()
            return

        # Group data by room_id and take highest available count
        grouped_data = {}
        for item in self.room_data:
            room_id = item['room_id']
            if room_id not in grouped_data or item['available_count'] > grouped_data[room_id]['available_count']:
                grouped_data[room_id] = item

        result_data = list(grouped_data.values())
        print(tabulate(result_data, headers="keys") + '\n')

        with self.database_connection() as (connection, cursor):
            try:
                # Start transaction
                connection.start_transaction()
                
                # First, mark all rooms for this booking_id as inactive
                cursor.execute(
                    "UPDATE rooms_id SET active = FALSE WHERE booking_id = %s",
                    (booking_id,)
                )
                
                # Then insert/update the current rooms
                data_to_insert = [
                    (item['room_id'], booking_id, item['room_type'], 
                     item['available_count'], True, item['price'])
                    for item in result_data
                ]
                
                query = """
                    INSERT INTO rooms_id 
                        (room_id, booking_id, room_type, max_available, active, price)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        room_type = VALUES(room_type),
                        max_available = VALUES(max_available),
                        active = VALUES(active),
                        price = VALUES(price)
                """
                cursor.executemany(query, data_to_insert)
                
                # Commit transaction
                connection.commit()
                
            except mysql.connector.Error as err:
                print(f"Database error: {err}")
                connection.rollback()
                raise
            
        # Clear room data after successful write
        self.room_data = []

    def start_requests(self):
        """Initialize spider with database connection and start requests"""
        sql = 'SELECT id, link FROM booking_data'
        
        if self.mode == 'priority':
            sql = 'SELECT id, link FROM booking_data WHERE priority > 0'
        elif self.mode and self.mode.isdigit():
            sql = f'SELECT id, link FROM booking_data WHERE id = {self.mode}'

        with self.database_connection() as (connection, cursor):
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                formatted_link = self.format_link(row['link'], self.checkin[0], self.checkout[0])
                yield scrapy.Request(
                    url=formatted_link,
                    callback=self.parse,
                    meta={
                        'booking_id': row['id'],
                        'link': row['link'],
                        'checkin': self.checkin[1],
                        'checkout': self.checkout[1],
                        'index': 1
                    },
                    errback=self.handle_error
                )

    def handle_error(self, failure):
        """Handle request failures"""
        booking_id = failure.request.meta.get('booking_id')
        print(f"Request failed for booking_id {booking_id}: {failure.value}")
        # Mark all rooms as inactive for this booking_id
        self.write_to_db(booking_id)

    def parse(self, response):
        """Parse booking.com response and extract room information"""
        booking_id = response.meta['booking_id']
        link = response.meta['link']
        checkin = response.meta['checkin']
        checkout = response.meta['checkout']
        index = response.meta['index']

        print(f'\nProcessing booking_id: {booking_id}\n')

        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')
        error_message = response.css('p.error[rel="this_hotel_is_not_bookable"]').get()
        alert_title = response.css('.bui-alert__title::text').get()

        if rows:
            self._parse_rooms(rows, response)
            
            if index < len(self.checkin) - 1:
                yield self._create_next_request(link, booking_id, index)
            else:
                self.write_to_db(booking_id)
                
        elif error_message:
            print(f'\n{booking_id} this_hotel_is_not_bookable\n')
            self.write_to_db(booking_id)
            
        elif alert_title:
            yield self._handle_alert(response, alert_title, booking_id, link, checkin, checkout, index)
            
        else:
            print(f'\n{booking_id} Something wrong! (301)')
            self.write_to_db(booking_id)

    def _parse_rooms(self, rows, response):
        """Helper method to parse room information from response"""
        for i, row in enumerate(rows):
            if row.xpath('./td/@rowspan').get():
                room_data = self._extract_room_data(row, response, i)
                if room_data:
                    self.room_data.append(room_data)

    def _extract_room_data(self, row, response, index):
        """Extract room data from a single row"""
        room_id = row.xpath('.//a[@class="hprt-roomtype-link"]/@data-room-id').get()
        room_type = row.xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get()
        
        if not room_id or not room_type:
            return None
            
        price = response.xpath(f'//*[@id="hprt-table"]/tbody/tr[{index + 1}]/td[3]/div/div/div[1]/div[2]/div/span/text()').get()
        price = ''.join(filter(str.isdigit, price)) if price else None
        
        max_available = row.xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get()
        count = int(max_available) if max_available else 0
        
        return {
            'booking_id': response.meta['booking_id'],
            'room_id': room_id,
            'room_type': room_type.strip(),
            'available_count': count,
            'price': price
        }

    def _create_next_request(self, link, booking_id, index):
        """Create request for the next date range"""
        return scrapy.Request(
            url=self.format_link(link, self.checkin[index + 1], self.checkout[index + 1]),
            callback=self.parse,
            meta={
                'booking_id': booking_id,
                'link': link,
                'checkin': self.checkin[index + 1],
                'checkout': self.checkout[index + 1],
                'index': index + 1
            },
            errback=self.handle_error
        )

    def _handle_alert(self, response, alert_title, booking_id, link, checkin, checkout, index):
        """Handle alert messages and create appropriate follow-up requests"""
        if 'is a minimum length of stay of' in alert_title:
            book_size = int(alert_title.split(' ')[-2])
            new_checkout = checkin + timedelta(days=book_size)
            
            return scrapy.Request(
                url=self.format_link(link, checkin, new_checkout),
                callback=self.parse,
                meta={
                    'booking_id': booking_id,
                    'link': link,
                    'checkin': checkin,
                    'checkout': new_checkout,
                    'index': index
                },
                errback=self.handle_error
            )
        elif index < len(self.checkin) - 1:
            return self._create_next_request(link, booking_id, index)
        else:
            self.write_to_db(booking_id)