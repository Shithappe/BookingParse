import scrapy
import mysql.connector
from datetime import datetime, timedelta
# from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
import time


class UpdateRoomsSpider(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        super(UpdateRoomsSpider, self).__init__(*args, **kwargs)

        self.today = datetime.now().date()
        self.checkin = [datetime.now().date() + timedelta(hours=5) + timedelta(weeks=i) for i in range(1, 4)]
        self.checkout = [self.checkin[i] + timedelta(days=1) for i in range(len(self.checkin))]
        self.max_value = []


    name = "rooms_3_week"
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
            cnx = mysql.connector.connect(**config)
            return cnx
        except mysql.connector.Error as err:
            print(f"Ошибка подключения к базе данных: {err}")

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
        
    def start_requests(self):
        self.connection = self.connect_to_db()
        if self.connection and self.connection.is_connected():
            print('\nConnection to DB success\n')
        else:
            raise SystemExit("Failed to connect to DB")
        
        self.cursor = self.connection.cursor()

        rows = None

        # self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id = 7992')
        self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id = 7992')
        rows = self.cursor.fetchall()


        for row in rows:
            for i in range(len(self.checkin)):
                formatted_link = self.format_link(row[1], self.checkin[i], self.checkout[i]) 
                request_meta = {
                    'booking_id': row[0],
                    'checkin': self.checkin[i], 
                    'checkout': self.checkout[i],
                    'is_last': i == len(self.checkin) - 1
                }
                # if request_meta['is_last']:
                #     time.sleep(2)
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)


    def parse(self, response):

        booking_id = response.meta.get('booking_id')
        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')
        is_last = response.meta.get('is_last')

        room_type = None
        max_available_rooms = None
        rowspan = None  
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        room_types_count = {}

        for i in range(len(rows)):
            rowspan = rows[i].xpath('./td/@rowspan').get()
            if rowspan:
                room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()
                max_available_rooms = rows[i].xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get() 
                if not max_available_rooms: 
                    max_available_rooms = 0

                count = int(max_available_rooms)

                if room_type in room_types_count:
                    room_types_count[room_type] += count
                else:
                    room_types_count[room_type] = count

        self.max_value.append(room_types_count)

        # if is_last and self.max_value:
        #     max_objects = max(self.max_value, key=lambda x: max(x.values(), default=0))
        #     print(max_objects)
        #     self.max_value = []
                    
        for room_type, count in room_types_count.items():
            print(f'{room_type}: {count}')
            self.cursor.execute("""
                INSERT INTO rooms_30_day
                (booking_id, room_type, max_available_rooms, checkin, checkout)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                booking_id, room_type, count, checkin, checkout
            ))
        self.connection.commit()
