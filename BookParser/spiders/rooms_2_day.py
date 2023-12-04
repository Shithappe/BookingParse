import scrapy
import mysql.connector
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse



class UpdateRoomsSpider(scrapy.Spider):
    
    checkin = datetime.now().date() + timedelta(hours=5)  # + relativedelta(days=1)  
    checkout = checkin + relativedelta(days=1)        

    name = "rooms_2_day"
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
            cnx = mysql.connector.connect(**config_local)
            # cnx = mysql.connector.connect(**config)
            return cnx
        except mysql.connector.Error as err:
            print(err)

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


    def parse(self, response):

        booking_id = response.meta.get('booking_id')
        room_type = None
        max_available_rooms = None
        rowspan = None
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        for i in range(len(rows)):
            rowspan = rows[i].xpath('./td/@rowspan').get()
            if rowspan:
                room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()
                for j in range(int(rowspan)):
                    max_available_rooms = rows[j + i].xpath('(//select[@class="hprt-nos-select js-hprt-nos-select"]//option)[last()]/@value').get()
                    if not max_available_rooms: 
                        max_available_rooms = 0

                    self.cursor.execute("""
                            INSERT INTO rooms_2_day
                            (booking_id, room_type, max_available_rooms, checkin, checkout)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                        booking_id, room_type, max_available_rooms, self.checkin, self.checkout
                    ))
                self.connection.commit()
