import scrapy
import mysql.connector
from tabulate import tabulate
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class Rooms_ID_Spider(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        super(Rooms_ID_Spider, self).__init__(*args, **kwargs)

        self.today = datetime.now().date()
        # self.checkin = [self.today + timedelta(days=2 + i) for i in range(21)]
        # self.checkin = [self.today + timedelta(days=60 + i) for i in range(2)]
        # self.checkout = [self.checkin[i] + timedelta(days=1) for i in range(len(self.checkin))]

        self.checkin, self.checkout = self.get_monthly_week_dates()


    name = "rooms_id"
    allowed_domains = ["www.booking.com"]
    start_urls = ["https://www.booking.com"]
    connection = None
    cursor = None
    

    def connect_to_db(self):
        config = {
            'user': 'artnmo_estate',
            'password': 'gL8+8uBs2_',
            'host': 'artnmo.mysql.tools',
            'database': 'artnmo_estate',
            'raise_on_warnings': True
        }
        
        try:
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            return connection, cursor

        except mysql.connector.Error as err:
            print(err)

    def format_link(self, link, checkin, checkout):
        url = urlparse(link)
        query_parameters = parse_qs(url.query)

        query_parameters['group_adults'] = 1
        query_parameters['no_rooms'] = 1
        query_parameters['group_children'] = 0

        query_parameters['checkin'] = checkin
        query_parameters['checkout'] = checkout

        query_parameters['selected_currency'] = 'USD'

        url = url._replace(query=urlencode(query_parameters, doseq=True))
        return urlunparse(url)

    def get_monthly_week_dates(self, num_months=3):
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
        
        checkout = [checkin[i] + timedelta(days=1) for i in range(len(checkin))]

        return checkin, checkout

    def write_to_db(self, booking_id, rooms):

        grouped_data = {}

        for item in self.room_data:
            room_id = item['room_id']
            if room_id not in grouped_data:
                grouped_data[room_id] = item
            else:
                if item['available_count'] > grouped_data[room_id]['available_count']:
                    grouped_data[room_id]['available_count'] = item['available_count']


        result_data = list(grouped_data.values())
        print(tabulate(result_data, headers="keys") + '\n')

        for item in result_data:
            item['active'] = True

        data_to_insert = [(item['room_id'], item['booking_id'], item['room_type'], item['available_count'], item['active'], item['price']) for item in result_data]

        query = """
            INSERT INTO rooms_id (room_id, booking_id, room_type, max_available, active, price)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                room_type=VALUES(room_type),
                max_available=VALUES(max_available),
                active=VALUES(active),
                price=VALUES(price)
        """
        self.cursor.executemany(query, data_to_insert)
        self.connection.commit()

        return


    def start_requests(self):
        self.connection, self.cursor = self.connect_to_db()

        self.cursor.execute("SELECT id, link FROM booking_data")
        rows = self.cursor.fetchall()

        self.room_data = []

        for row in rows:
            try:
                self.connection.start_transaction()

                select_query = "SELECT room_id, booking_id, room_type, max_available, price FROM rooms_id WHERE booking_id = %s"
                self.cursor.execute(select_query, (row[0],))
                rooms = self.cursor.fetchall()

                update_query = "UPDATE rooms_id SET active = %s WHERE booking_id = %s"
                self.cursor.execute(update_query, (False, row[0]))

                self.connection.commit()

            except mysql.connector.Error as err:
                self.connection.rollback()
                print(f"Error: {err}")

            formatted_link = self.format_link(row[1], self.checkin[0], self.checkout[0]) 
            request_meta = {
                'booking_id': row[0],
                'link': row[1],
                'rooms': rooms,
                'checkin': self.checkin[1], 
                'checkout': self.checkout[1],
                'index': 1
            }
            yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)

    def parse(self, response):
        booking_id = response.meta.get('booking_id')
        link = response.meta.get('link')
        rooms = response.meta.get('rooms')
        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')
        index = response.meta.get('index')

        room_type = None
        max_available_rooms = None
        rowspan = None  

        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        if rows:
            for i in range(len(rows)):
                rowspan = rows[i].xpath('./td/@rowspan').get()
                if rowspan:
                    room_id = rows[i].xpath('.//a[@class="hprt-roomtype-link"]/@data-room-id').get()
                    room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()

                    price = response.xpath(f'//*[@id="hprt-table"]/tbody/tr[{i+1}]/td[3]/div/div/div[1]/div[2]/div/span/text()').get()
                    if price:
                        price = ''.join(filter(str.isdigit, price))

                    max_available_rooms = rows[i].xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get()
                    if not max_available_rooms:
                        max_available_rooms = 0

                    count = int(max_available_rooms)

                    self.room_data.append({'booking_id': booking_id, 'room_id': room_id, 'room_type': room_type, 'available_count': count, 'price': price})



            if index != len(self.checkin) - 1:
                formatted_link = self.format_link(link, checkin, checkout)
                request_meta = {
                    'booking_id': booking_id,
                    'link': link,
                    'rooms': rooms,
                    'checkin': self.checkin[index + 1],
                    'checkout': self.checkout[index + 1],
                    'index': index + 1
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)
            else:
                self.write_to_db(booking_id, rooms)

        else:
            alert_title = response.css('.bui-alert__title::text').get()
            print(alert_title)

            if 'is a minimum length of stay of' in alert_title:
                book_size = int(alert_title.split(' ')[-2])
                checkout = checkin + timedelta(days=book_size)

                formatted_link = self.format_link(response.meta.get('link'), checkin, checkout)

                request_meta = {
                    'booking_id': booking_id,
                    'link': response.meta.get('link'),
                    'rooms': rooms,
                    'checkin': checkin,
                    'checkout': checkout,
                    'index': index
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)

            elif index != len(self.checkin) - 1:
                formatted_link = self.format_link(link, checkin, checkout)
                request_meta = {
                    'booking_id': booking_id,
                    'link': link,
                    'rooms': rooms,
                    'checkin': self.checkin[index + 1],
                    'checkout': self.checkout[index + 1],
                    'index': index + 1
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)
            else:
                self.write_to_db(booking_id, rooms)
