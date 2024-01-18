import scrapy
import mysql.connector
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class UpdateRoomsSpider(scrapy.Spider):    

    today = datetime.now().date()

    checkin = (datetime.now() + timedelta(hours=8)).date()
    checkout = (datetime.now() + timedelta(hours=8) + timedelta(days=1)).date()

    name = "remaining_rooms"
    allowed_domains = ["www.booking.com"]
    start_urls = ["https://www.booking.com"]
    connection = None
    cursor = None
    

    def connect_to_db(self):
        
        config_local = {
            'user': 'root',
            'password': '1234',
            'host': 'localhost',
            'database': 'parser_booking',
            'raise_on_warnings': True
        }

        config = {
            'user': 'artnmo_estate',
            'password': 'gL8+8uBs2_',
            'host': 'artnmo.mysql.tools',
            'database': 'artnmo_estate',
            'raise_on_warnings': True
        }
        
        try:
            # cnx = mysql.connector.connect(**config_local)
            cnx = mysql.connector.connect(**config)
            return cnx
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
        
    def start_requests(self):
        self.connection = self.connect_to_db()
        if self.connection and self.connection.is_connected():
            print('\nConnection to DB success\n')
        else:
            print('\nFailed to connect to DB. Exiting...\n')
            raise SystemExit("Failed to connect to DB")
        
        self.cursor = self.connection.cursor()

        rows = None
        self.cursor.execute(f'SELECT id, link FROM booking_data')
        rows = self.cursor.fetchall()


        for row in rows:
                formatted_link = self.format_link(row[1], self.checkin, self.checkout) 
                request_meta = {
                    'booking_id': row[0],
                    'checkin': self.checkin, 
                    'checkout': self.checkout
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)


    def parse(self, response):

        booking_id = response.meta.get('booking_id')
        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')        
        
        room_type = None
        available_rooms = None
        rowspan = None
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        for i in range(len(rows)):
            rowspan = rows[i].xpath('./td/@rowspan').get()
            if rowspan:
                room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()
                available_rooms = rows[i].xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get()                
                if not available_rooms: 
                    available_rooms = 0

                # print(f'\n{room_type, available_rooms}\n')

                self.cursor.execute("""
                        INSERT INTO remaining_rooms
                        (booking_id, room_type, available_rooms, checkin, checkout)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                    booking_id, room_type, available_rooms, checkin, checkout
                ))
            self.connection.commit()
