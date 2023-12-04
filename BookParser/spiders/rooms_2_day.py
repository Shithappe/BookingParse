import scrapy
import mysql.connector
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse



class UpdateRoomsSpider(scrapy.Spider):
    
    checkin = datetime.now().date() + timedelta(hours=5) + relativedelta(days=1)   # today
    checkout = checkin + relativedelta(days=1)          # tomorrow


    name = "rooms_2_day"
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


        # self.cursor.execute('''
        #     SELECT sub_query.room_id AS room_id, sub_query.room_count AS room_count, bd.link AS link
        #     FROM (
        #         SELECT r.booking_id, MIN(r.id) AS room_id, COUNT(r.id) AS room_count
        #         FROM rooms r
        #         GROUP BY r.booking_id
        #     ) AS sub_query
        #     INNER JOIN booking_data bd ON sub_query.booking_id = bd.id
        # ''')
        # rows = self.cursor.fetchall()
        # for row in rows:
        #     formatted_link = self.format_link(row[2]) 
            
        #     request_meta = {
        #         'room_id': row[0],
        #         'count_room': row[1],
        #     }

        #     yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)

        # self.cursor.close()
        # self.connection.close()

    def parse(self, response):

        # room_id = response.meta.get('room_id')
        # count_room = response.meta.get('count_room')

        booking_id = response.meta.get('booking_id')

        self.sql = """
            INSERT INTO rooms_2_day
            (booking_id, max_available_rooms, checkin, checkout)
            VALUES (%s, %s, %s, %s)
        """

        max_available_rooms = None
        rowspan = None
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        for i in range(len(rows)):
            rowspan = rows[i].xpath('./td/@rowspan').get()
            if rowspan:
                for j in range(int(rowspan)):
                    
                    max_available_rooms = rows[j + i].xpath('(//select[@class="hprt-nos-select js-hprt-nos-select"]//option)[last()]/@value').get()
                    if max_available_rooms is None:
                        max_available_rooms = 0

                    self.cursor.execute(self.sql, (
                        booking_id, max_available_rooms, self.checkin, self.checkout
                    ))
                self.connection.commit()
