# Этот парсер обновляет данные по комнатам 
# booking_id, room_type, activity, price в таблицу rooms
# парсер должен запускатсья +- раз в неделю, суть отсеивать не актуальные комнаты, если какую-то комнут парсер не находит она не должна отобрадаться 
# получить ссылки отелей, получить комныты этого отеля, получить названия комныт и их размерность, установить active = false при отсудствии, обновить размерность, цену при расхождении
###############################################################

import scrapy
import mysql.connector
from tabulate import tabulate
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class UpdateRoomsSpider(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        super(UpdateRoomsSpider, self).__init__(*args, **kwargs)

        self.today = datetime.now().date()
        self.checkin = [self.today + timedelta(days=2 + i) for i in range(21)]
        # self.checkin = [self.today + timedelta(days=7 + i) for i in range(7)]
        self.checkout = [self.checkin[i] + timedelta(days=1) for i in range(len(self.checkin))]

    

    name = "update_rooms"
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
        
    def write_to_db(self, booking_id, rooms):
        if len(self.room_data):
            print('\nWRITE TO DB\n')

            headers = ['Booking ID', 'Title', 'Available Count', 'Price', 'Active']

            new_data_dict = {}
            for item in self.room_data:
                if item['booking_id'] == booking_id:
                    room_type = item['title']
                    if room_type not in new_data_dict:
                        new_data_dict[room_type] = item
                    else:
                        if item['available_count'] > new_data_dict[room_type]['available_count']:
                            new_data_dict[room_type]['available_count'] = item['available_count']
                        new_data_dict[room_type]['price'] = item['price']

            print('\nOld data')
            rows = [[item[0], item[1], item[2], item[3]] for item in rooms if item[0] == booking_id]
            print(tabulate(rows, headers[:-1]) + '\n')

            print('\nNew data')
            updated_rows = []
            existing_room_types = {item[1] for item in rooms if item[0] == booking_id}

            for item in rooms:
                if item[0] == booking_id:
                    room_type = item[1]
                    if room_type in new_data_dict:
                        new_item = new_data_dict[room_type]
                        updated_rows.append([item[0], item[1], new_item['available_count'], new_item['price'], True])
                    else:
                        updated_rows.append([item[0], item[1], item[2], item[3], False])

            data_to_insert = []
            for room_type, new_item in new_data_dict.items():
                if room_type not in existing_room_types:
                    updated_rows.append([booking_id, new_item['title'], new_item['available_count'], new_item['price'], True])
                    data_to_insert.append((new_item['booking_id'], new_item['title'], new_item['available_count'], True, new_item['price']))

            if data_to_insert:
                query = """
                    INSERT INTO rooms
                    (booking_id, room_type, max_available, active, price)
                    VALUES (%s, %s, %s, %s, %s)
                """
                self.cursor.executemany(query, data_to_insert)
                self.connection.commit()

            data_to_update = []
            for room_type, new_item in new_data_dict.items():
                if room_type in existing_room_types:
                    data_to_update.append((new_item['available_count'], new_item['price'], True, booking_id, room_type))

            if data_to_update:
                query = """
                    UPDATE rooms
                    SET max_available = %s, price = %s, active = %s
                    WHERE booking_id = %s AND room_type = %s
                """
                self.cursor.executemany(query, data_to_update)
                self.connection.commit()

            print(tabulate(updated_rows, headers) + '\n')

            self.room_data = []


    def start_requests(self):
        self.connection = self.connect_to_db()
        if self.connection and self.connection.is_connected():
            print('\nConnection to DB success\n')
        else:
            raise SystemExit("Failed to connect to DB")
        
        self.cursor = self.connection.cursor()
        print(len(self.checkin))

        # self.cursor.execute("SELECT id, link FROM booking_data where id = 2075")
        self.cursor.execute("SELECT id, link FROM booking_data")
        rows = self.cursor.fetchall()

        self.room_data = []

        for row in rows:
            try:
                self.connection.start_transaction()

                select_query = "SELECT booking_id, room_type, max_available, price FROM rooms WHERE booking_id = %s"
                self.cursor.execute(select_query, (row[0],))
                rooms = self.cursor.fetchall()

                update_query = "UPDATE rooms SET active = %s WHERE booking_id = %s"
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
                    room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()

                    price = response.xpath(f'//*[@id="hprt-table"]/tbody/tr[{i+1}]/td[3]/div/div/div[1]/div[2]/div/span/text()').get()
                    if price:
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

                    if not room_exists and count > 0:
                        self.room_data.append({'booking_id': booking_id, 'title': room_type, 'available_count': count, 'price': price})

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
