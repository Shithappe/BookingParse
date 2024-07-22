import re
import scrapy
import mysql.connector

from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class MySpider(scrapy.Spider):

    next_month = datetime.now().date() + relativedelta(months=1)
    checkin = next_month
    checkout = next_month + relativedelta(days=1)


    name = "booking_update"
    allowed_domains = ["www.booking.com"]
    start_urls = []
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
            raise SystemExit("Failed to connect to DB")
        
        self.cursor = self.connection.cursor()

        self.cursor.execute("SELECT id, link FROM booking_data WHERE images = '[]'")
        booking_data = self.cursor.fetchall()

        for book in booking_data:
            id, url = book[0], book[1]
            yield scrapy.Request(url=self.format_link(url), callback=self.parse, meta={'id': id, 'original_url': url})

    def parse(self, response):

        id = response.meta.get('id')

        images = response.css('a[data-thumb-url]::attr(data-thumb-url)').extract()
        # small_images = response.css('a.bh-photo-grid-item.bh-photo-grid-thumb > img::attr(src)').extract()
        # images.extend(small_images)
        # images = [image.replace('max300', 'max500') for image in images]
        print(id)
        print(images)

        sql = """
             UPDATE booking_data 
            SET images = %s
            WHERE id = %s
        """
        self.cursor.execute(sql, (
            str(images), id
        ))
        self.connection.commit()