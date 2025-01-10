# сбор/обновление информации об отеле

import re
import os
import scrapy
import mysql.connector

from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class MySpider(scrapy.Spider):

    def __init__(self, mode = None, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.mode = mode

    next_month = datetime.now().date() + relativedelta(months=1)
    checkin = next_month
    checkout = next_month + relativedelta(days=1)


    name = "booking"
    allowed_domains = ["www.booking.com"]
    start_urls = []
    connection = None
    cursor = None
        
    
    def connect_to_db(self):
        config = {
            'user': os.getenv('DATABASE_USER'),
            'password': os.getenv('DATABASE_PASSWORD'),
            'host': os.getenv('DATABASE_HOST'),
            'database': os.getenv('DATABASE_NAME'),
            'raise_on_warnings': True
        }
        
        try:
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            return connection, cursor

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
        sql = 'SELECT id, link FROM booking_data'

        if self.mode == 'priority':
            print(f"\n\nMode: {self.mode}")
            sql = 'SELECT id, link FROM booking_data where priority > 0'
        
        if self.mode and self.mode.isdigit():
            print(f"\n\nMode: ID")
            sql = f'SELECT id, link FROM booking_data where id = {self.mode}'
        
        self.connection, self.cursor = self.connect_to_db()

        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        for row in rows:
            request_meta = {
                'booking_id': row[0]
            }
            yield scrapy.Request(url=self.format_link(row[1]), callback=self.parse, meta=request_meta)

        # self.cursor.close()
        # self.connection.close()

    def parse(self, response):
        booking_id = response.meta.get('booking_id')

        title = response.css('.d2fee87262.pp-header__title::text').extract_first()
        description = response.css('.a53cbfa6de.b3efd73f69::text').extract_first().strip()

        star = len(response.css('span[data-testid="rating-stars"] > span'))
        address = response.css('div.a53cbfa6de.f17adf7576::text').extract_first().strip()

        street_address = ''
        city = ''
        country = ''

        parts = address.split(', ')
        
        if len(parts) >= 3:
            # Последний элемент - страна
            country = parts[-1]
            
            # Предпоследний элемент может содержать почтовый индекс и город
            city_parts = parts[-2].split(' ', 1)  # Разделяем на индекс и название города
            if len(city_parts) > 1:
                city = city_parts[1]  # Берем название города без индекса
            else:
                city = city_parts[0]
                
            # Все остальные части - это адрес
            street_parts = parts[:-2]
            street_address = ', '.join(street_parts)

        location = next((
            a.attrib.get('data-atlas-latlng')
            for a in response.css('a')
            if 'data-atlas-latlng' in a.attrib
        ), None)

        score = response.css('div.a3b8729ab1.d86cee9b25::text').get()

        # images = response.css('a[data-thumb-url]::attr(data-thumb-url)').extract()
        # small_images = response.css('a.bh-photo-grid-item.bh-photo-grid-thumb > img::attr(src)').extract()
        # images.extend(small_images)

        images = response.css('picture > img::attr(src)').extract()
        filtered_images = [image for image in images if "https://cf.bstatic.com/xdata/images/" in image]
        filtered_images = [image.replace('max300', 'max500').replace('max1024x768', 'max500') for image in filtered_images]
        images_str = ','.join(filtered_images)
        # for i in range(len(filtered_images)):
        #     print(f'{i + 1}: {filtered_images[i]}')

        type = None
        avalible_types = ["Villa", "Villas", "Guesthouse", "Homestay", "Bungalows", "Resort"]
        
        for word in avalible_types:
            if word in title:
                type = word

        if type is None:
            type = response.css('.bui-breadcrumb__text a.bui_breadcrumb__link::text').getall()[2]
            type_match = re.search(r'All\s+(.+)', type)
            type = (type_match.group(1) if type_match else 'Hotel').capitalize()


        review_count = response.xpath('//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[2]/div[2]/text()').get()
        if review_count:
            review_count = int(review_count.split()[-2].replace(',', ''))

        # price = response.css(".prco-valign-middle-helper::text").get()
        # if (price):
        #     price = int(re.search(r'\d+', price).group())


        sql = """
            UPDATE booking_data
            SET 
                title = %s,
                description = %s,
                star = %s,
                address = %s,
                city = %s,
                country = %s,
                location = %s,
                score = %s,
                images = %s,
                type = %s,
                review_count = %s
            WHERE id = %s
        """
        self.cursor.execute(sql, (
            title, description, star, address, city, country, location, score, images_str, type, review_count, booking_id
        ))

        self.connection.commit()
