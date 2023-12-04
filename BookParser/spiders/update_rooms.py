import re
import scrapy
import mysql.connector
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse



class UpdateRoomsSpider(scrapy.Spider):

    next_month = datetime.now().date() + relativedelta(months = 3)
    checkin = next_month
    checkout = next_month + relativedelta(days = 1)


    name = "update_rooms"
    allowed_domains = ["www.booking.com"]
    start_urls = ["https://www.booking.com"]
    connection = None
    cursor = None
    sql = None
    

    def connect_to_db(self):
        
        # config = {
        #     'user': 'root',
        #     'password': '1234',
        #     'host': 'localhost',
        #     'database': 'parser_booking',
        #     'raise_on_warnings': True
        # }

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

    def format_link(self, link):
        url = urlparse(link)
        query_parameters = parse_qs(url.query)

        query_parameters['group_adults'] = 1
        query_parameters['no_rooms'] = 1
        query_parameters['group_children'] = 0

        query_parameters['checkin'] = self.checkin
        query_parameters['checkout'] = self.checkout

        query_parameters['selected_currency'] = 'USD'

        url = url._replace(query=urlencode(query_parameters, doseq=True))
        print('\n'+urlunparse(url)+'\n')

        return urlunparse(url)
        
    def start_requests(self):
        self.connection = self.connect_to_db()
        if self.connection and self.connection.is_connected():
            print('\nConnection to DB success\n')
        else:
            print('\nFailed to connect to DB. Exiting...\n')
            raise SystemExit("Failed to connect to DB")
        
        self.cursor = self.connection.cursor()

        self.cursor.execute('SELECT id, link FROM booking_data')
        rows = self.cursor.fetchall()
        for row in rows:
            formatted_link = self.format_link(row[1]) 
            request_meta = {
                'booking_id': row[0],
                'link': formatted_link
            }
            yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)

        # self.cursor.close()
        # self.connection.close()

    def parse(self, response):

        booking_id = response.meta.get('booking_id')
        link = response.meta.get('link')
        print(f'\n{booking_id}\n{link}\n\n')

        sql_select = "SELECT id, max_people, max_available_rooms FROM rooms WHERE booking_id = %s"
        self.cursor.execute(sql_select, (booking_id,))
        old_rooms = self.cursor.fetchall()
        if old_rooms:
            print(old_rooms)
            self.sql = """
                UPDATE rooms 
                SET max_people = %s, prices = %s, max_available_rooms = %s WHERE booking_id = %s
            """
        else:
            print("Нет данных для данного booking_id")
            self.sql = """
                INSERT INTO rooms 
                (booking_id, title, max_people, prices, max_available_rooms, checkin, checkout)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """


        checkin = None
        checkout = None

        url = urlparse(link)
        query_parameters = parse_qs(url.query)

        checkin = query_parameters.get('checkin')[0]
        checkout = query_parameters.get('checkout')[0]
        
        title = None
        max_people = None
        prices = None
        max_available_rooms = None
        rowspan = None
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        for i in range(len(rows)):
            rowspan = rows[i].xpath('./td/@rowspan').get()
            if rowspan:
                title = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()

                for j in range(int(rowspan)):
                    text = rows[j + i].xpath('.//span[@class="bui-u-sr-only"]/text()').get().split(':')[-1].strip()
                    numbers = [int(num) for num in re.findall(r'\d+', text)]
                    if old_rooms:
                        max_people = max(old_rooms[i + j][1], max(numbers))
                    else:
                        max_people = max(numbers)

                    prices = re.sub(r'[^\d.]', '', rows[j + i].xpath('.//span[@class="prco-valign-middle-helper"]/text()').get())

                    if old_rooms:
                        max_available_rooms = max(int(old_rooms[i + j][2]), int(rows[j + i].xpath('(//select[@class="hprt-nos-select js-hprt-nos-select"]//option)[last()]/@value').get()))
                    else:
                        max_available_rooms = rows[j + i].xpath('(//select[@class="hprt-nos-select js-hprt-nos-select"]//option)[last()]/@value').get()

                    print(f'\n\n{booking_id}\n{link}\n{title}\n{max_people}\n{prices}\n{max_available_rooms}\n{checkin} - {checkout}')
                    
                    if old_rooms:
                        self.cursor.execute(self.sql, (max_people, prices, max_available_rooms, i + j))
                    else:
                        self.cursor.execute(self.sql, (
                            booking_id, title, max_people, prices, max_available_rooms, checkin, checkout
                        ))
                    self.connection.commit()
