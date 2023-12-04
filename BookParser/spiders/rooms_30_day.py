import scrapy
import mysql.connector
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse



class UpdateRoomsSpider(scrapy.Spider):
    
    checkin = [datetime.now().date() + timedelta(hours=5) + timedelta(days=i) for i in range(30)]
    checkout = [datetime.now().date() + timedelta(hours=5) + timedelta(days=i+1) for i in range(30)]   


    name = "rooms_30_day"
    allowed_domains = ["www.booking.com"]
    start_urls = ["https://www.booking.com"]
    connection = None
    cursor = None
    

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
            print('\nFailed to connect to DB. Exiting...\n')
            raise SystemExit("Failed to connect to DB")
        
        self.cursor = self.connection.cursor()

        self.cursor.execute('SELECT id, link FROM booking_data')
        rows = self.cursor.fetchall()


        for row in rows:
            for i in range(30):
                formatted_link = self.format_link(row[1], self.checkin[i], self.checkout[i]) 
                request_meta = {
                    'booking_id': row[0],
                    'link': formatted_link,
                    'checkin': self.checkin[i], 
                    'checkout': self.checkout[i]
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta, priority=i)

    def parse(self, response):

        booking_id = response.meta.get('booking_id')
        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')

        link = response.meta.get('link')



        rowspan = None
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        for i in range(len(rows)):
            rowspan = rows[i].xpath('./td/@rowspan').get()
            if rowspan:
                for j in range(int(rowspan)):
                    max_available_rooms = None
                    
                    try:
                        # Ваш код для извлечения данных
                        max_available_rooms = rows[j + i].xpath('(//select[@class="hprt-nos-select js-hprt-nos-select"]//option)[last()]/@value').get()
                        if max_available_rooms is None:
                            max_available_rooms = 0
                        # Другие операции с извлеченными данными
                    except Exception as e:
                        print(f"Произошла ошибка: {e}")
                        # Действия при отсутствии нужных элементов на странице
                        max_available_rooms = 0 

                    self.cursor.execute("""
                            INSERT INTO rooms_30_day
                            (booking_id, max_available_rooms, checkin, checkout)
                            VALUES (%s, %s, %s, %s)
                        """, (
                        booking_id, max_available_rooms, checkin, checkout
                    ))
                    self.connection.commit()

                    print(f'\n{booking_id}\n{max_available_rooms}\n{link}\n\n')
