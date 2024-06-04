# Этот парсер собирает данные по комнатам 
# booking_id, room_type, activity, price в таблицу rooms
###############################################################

import re
import scrapy
import mysql.connector
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

from tabulate import tabulate


class UpdateRoomsSpider(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        super(UpdateRoomsSpider, self).__init__(*args, **kwargs)

        self.today = datetime.now().date()
        self.checkin = [self.today + timedelta(hours=5) + timedelta(weeks=3) + timedelta(days=i) for i in range(1, 8)]
        self.checkout = [self.checkin[i] + timedelta(days=1) for i in range(len(self.checkin))]


    name = "check_rooms"
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

        self.cursor.execute("SELECT id, link FROM booking_data where id = 4")
        # self.cursor.execute(f'SELECT id, link FROM booking_data')
        rows = self.cursor.fetchall()


        self.room_data =[]

        for row in rows:
            formatted_link = self.format_link(row[1], self.checkin[0], self.checkout[0]) 
            request_meta = {
                'booking_id': row[0],
                'link': row[1],
                'checkin': self.checkin[1], 
                'checkout': self.checkout[1],
                'index': 1
            }
            yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)


    def parse(self, response):

        booking_id = response.meta.get('booking_id')
        link = response.meta.get('link')
        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')
        index = response.meta.get('index')

        # print('\n', booking_id, checkin, checkout, index)

        room_type = None
        max_available_rooms = None
        rowspan = None  
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        if (rows):
            for i in range(len(rows)):
                rowspan = rows[i].xpath('./td/@rowspan').get()
                if rowspan:
                    room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()

                    price = response.xpath(f'//*[@id="hprt-table"]/tbody/tr[{i+1}]/td[3]/div/div/div[1]/div[2]/div/span/text()').get()
                    if (price):
                        price = ''.join(filter(str.isdigit, price))

                    max_available_rooms = rows[i].xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get() 
                    if not max_available_rooms: 
                        max_available_rooms = 0

                    count = int(max_available_rooms)


                    room_exists = False
                    for room in self.room_data:
                        if room['title'] == room_type:
                            room_exists = True
                            if count > room['available_count']:
                                room['available_count'] = count
                            break

                    if not room_exists:
                        self.room_data.append({'booking_id': booking_id, 'title': room_type, 'available_count': count, 'price': price})


            if index != len(self.checkin) - 1:
                formatted_link = self.format_link(link, checkin, checkout) 
                request_meta = {
                    'booking_id': booking_id,
                    'link': link,
                    'checkin': self.checkin[index + 1], 
                    'checkout': self.checkout[index + 1],
                    'index': index + 1
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)
            else:
                if len(self.room_data):
                    print('\nWRITE TO DB\n')

                    headers = ['Booking ID', 'Title', 'Available Count', 'Price']
                    rows = [[item['booking_id'], item['title'], item['available_count'], item['price']] for item in self.room_data if item['booking_id'] == booking_id]
                    print('\n' + tabulate(rows, headers) + '\n')

                    # data_to_insert = [(room['booking_id'], room['title'], room['available_count'], True, room['price']) for room in self.room_data]
                    # if data_to_insert:
                    #     query = """
                    #         INSERT INTO rooms
                    #         (booking_id, room_type, max_available, active, price)
                    #         VALUES (%s, %s, %s, %s, %s)
                    #     """

                    #     self.cursor.executemany(query, data_to_insert)
                    #     self.connection.commit()

                    self.room_data = []


        else:
            alert_title = response.css('.bui-alert__title::text').get()
            print(alert_title)

            if 'is a minimum length of stay of' in alert_title:
                book_size = int(alert_title.split(' ')[-2])

                # checkin = self.today + timedelta(hours=8)
                checkout = checkin + timedelta(days=book_size)


                formatted_link = self.format_link(response.meta.get('link'), checkin, checkout) 

                request_meta = {
                    'booking_id': booking_id,
                    'link': response.meta.get('link'),
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
                    'checkin': self.checkin[index + 1], 
                    'checkout': self.checkout[index + 1],
                    'index': index + 1
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)