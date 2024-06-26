import re
import scrapy
import mysql.connector
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class UpdateRoomsSpider(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        super(UpdateRoomsSpider, self).__init__(*args, **kwargs)

        self.today = datetime.now().date()
        self.checkin = [datetime.now().date() + timedelta(hours=5) + timedelta(weeks=i) for i in range(1, 5)]
        self.checkout = [self.checkin[i] + timedelta(days=1) for i in range(len(self.checkin))]
        self.max_value = {}


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

        self.cursor.execute("SELECT id, link FROM booking_data WHERE id = 6741")
        # self.cursor.execute(f'SELECT id, link FROM booking_data')
        rows = self.cursor.fetchall()


        for row in rows:
            formatted_link = self.format_link(row[1], self.checkin[0], self.checkout[0]) 
            request_meta = {
                'booking_id': row[0],
                'link': row[1],
                'checkin': self.checkin[1], 
                'checkout': self.checkout[1],
                'index': 1,
                'max_value': {}
            }
            yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)


    def parse(self, response):

        booking_id = response.meta.get('booking_id')
        link = response.meta.get('link')
        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')
        index = response.meta.get('index')
        max_value = response.meta.get('max_value')

        room_type = None
        max_available_rooms = None
        rowspan = None  
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        room_types_count = {}

        if (rows):
            for i in range(len(rows)):
                rowspan = rows[i].xpath('./td/@rowspan').get()
                if rowspan:
                    room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()

                    price = response.xpath(f'//*[@id="hprt-table"]/tbody/tr[{i+1}]/td[3]/div/div/div[1]/div[2]/div/span/text()').get()
                    if (price):
                        price = int(re.search(r'\d+', price).group())
                    print(price)

                    max_available_rooms = rows[i].xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get() 
                    if not max_available_rooms: 
                        max_available_rooms = 0

                    count = int(max_available_rooms)

                    if room_type in room_types_count:
                        room_types_count[room_type] += count
                    else:
                        room_types_count[room_type] = count
                        # room_types_count['price'] = price


            if not max_value:
                max_value = room_types_count
            else:
                for room_type, count in room_types_count.items():
                    if room_type in max_value:
                        max_value[room_type] = max(max_value[room_type], count)
                    else:
                        max_value[room_type] = count


            print(booking_id, index, len(self.checkin) - 1)
            if index != len(self.checkin) - 1:
                formatted_link = self.format_link(link, checkin, checkout) 
                request_meta = {
                    'booking_id': booking_id,
                    'link': link,
                    'checkin': self.checkin[index + 1], 
                    'checkout': self.checkout[index + 1],
                    'index': index + 1,
                    'max_value': max_value
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)
            else:
                print(max_value)
                print('\nWRITE TO DB')
                if (max_value):
                    for room_type, count in max_value.items():
                        print(f'{room_type}: {count} -- {price}')
                    #     self.cursor.execute("""
                    #         INSERT INTO rooms_30_day
                    #         (booking_id, room_type, max_available_rooms, checkin, checkout, price)
                    #         VALUES (%s, %s, %s, %s, %s, %s)
                    #     """, (
                    #         booking_id, room_type, count, checkin, checkout, price
                    #     ))
                    # self.connection.commit()

        else:
            alert_title = response.css('.bui-alert__title::text').get()
            print(alert_title)

            if 'is a minimum length of stay of' in alert_title:
                book_size = int(alert_title.split(' ')[-2])

                checkin = self.today + timedelta(hours=8)
                checkout = checkin + timedelta(days=book_size)


                formatted_link = self.format_link(response.meta.get('link'), checkin, checkout) 

                request_meta = {
                    'booking_id': booking_id,
                    'link': response.meta.get('link'),
                    'checkin': checkin, 
                    'checkout': checkout
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)