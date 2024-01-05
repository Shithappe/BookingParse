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


    name = "booking_data"
    allowed_domains = ["www.booking.com"]
    start_urls = []
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

        self.cursor.execute('SELECT link FROM links')
        all_links = [i[0] for i in self.cursor.fetchall()]

        self.cursor.execute('SELECT link FROM booking_data')
        old_links = [i[0] for i in self.cursor.fetchall()]


        self.start_urls = [x for x in all_links if x not in old_links]
        # self.start_urls = [self.format_link(i[0]) for i in all_links]
        print(self.start_urls)

        for url in self.start_urls:
            yield scrapy.Request(url=self.format_link(url), callback=self.parse, meta={'original_url': url})

        self.cursor.close()
        self.connection.close()

    def parse(self, response):

        title = response.css('.d2fee87262.pp-header__title::text').extract_first()
        description = response.css('.a53cbfa6de.b3efd73f69::text').extract_first().strip()

        star = len(response.css('span[data-testid="rating-stars"] > span'))
        address = response.css('span.hp_address_subtitle::text').get().strip()
        if len(address.split(',')) > 3:
            city = re.sub(r'\d+', '', address.split(',')[3]).strip()
        else:
            city = re.sub(r'\d+', '', address.split(',')[1]).strip()

        location = response.css('a#hotel_address').attrib.get('data-atlas-latlng')

        score = response.css('div.a3b8729ab1.d86cee9b25::text').get()

        images = response.css('a.bh-photo-grid-item.bh-photo-grid-thumb > img::attr(src)').extract()

        link = response.meta.get('original_url').split('?')[0]

        type = response.css('li[itemprop="itemListElement"][data-google-track*="hotel"] a::text')[1].get()
        if 'All' in type:
            type = type.replace('All', '').strip().capitalize()
        else:
            type = 'Guest houses'


        sql = """
            INSERT INTO booking_data 
            (title, description, star, link, address, city, location, score, images, type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(sql, (
            title, description, star, link, address, city, location, score, str(images), type
        ))
        self.connection.commit()
