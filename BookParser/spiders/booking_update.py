import re
import scrapy
import mysql.connector

from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class MySpider(scrapy.Spider):

    next_month = datetime.now().date() + relativedelta(months=4)
    checkin = next_month
    checkout = next_month + relativedelta(days=1)


    name = "booking_update"
    allowed_domains = ["www.booking.com"]
    start_urls = []
    connection = None
    cursor = None

    facilities_cache = {}
        
    
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

        # self.cursor.execute('SELECT id, title FROM facilities')
        # facilities_data = self.cursor.fetchall()
        # self.facilities_cache = {title: id for id, title in facilities_data}

        self.cursor.execute('SELECT id, link FROM booking_data WHERE country = "Spain"')
        # self.cursor.execute('SELECT id, link FROM booking_data WHERE id = 12229')
        booking_data = self.cursor.fetchall()

        # print(len(booking_data))

        for book in booking_data:
            id, url = book[0], book[1]
            yield scrapy.Request(url=self.format_link(url), callback=self.parse, meta={'id': id, 'original_url': url.replace('.html', '.en-gb.html')})
        # url = 'https://www.booking.com/hotel/th/mitr-iinn-eyaawraach-mitr-inn-yaowarat.html'
        # yield scrapy.Request(url=self.format_link(url), callback=self.parse, meta={'id': 11463, 'original_url': url})

        # self.cursor.close()
        # self.connection.close()

    def parse(self, response):

        booking_id = response.meta.get('id')

        title = response.xpath('/html/body/div[4]/div/div[5]/div[1]/div[1]/div[1]/div[1]/div[2]/div[4]/div[1]/div/div/h2/text()').get()
        description = response.xpath('/html/body/div[4]/div/div[5]/div[1]/div[1]/div[2]/div/div/div[1]/div[1]/div[1]/div[1]/div[2]/div/div/p[1]/text()').get().strip()

        star = len(response.css('span[data-testid="rating-stars"] > span'))
        address = response.css('span.hp_address_subtitle::text').get().strip()
        city = response.css('input[name="ss"]::attr(value)').extract_first().strip()

        location = response.css('a#hotel_address').attrib.get('data-atlas-latlng')

        score = response.xpath('/html/body/div[4]/div/div[5]/div[1]/div[1]/div[1]/div[1]/div[4]/div/div[1]/div[1]/div/div[1]/a/div/div/div/div[1]/text()').get().strip()

        images = response.css('a[data-thumb-url]::attr(data-thumb-url)').extract()
        small_images = response.css('a.bh-photo-grid-item.bh-photo-grid-thumb > img::attr(src)').extract()
        images.extend(small_images)

        images = [image.replace('max300', 'max500') for image in images]


        type = None
        avalible_types = ["Villa", "Villas", "Guesthouse", "Homestay", "Bungalows", "Resort"]
        
        for word in avalible_types:
            if word in title:
                type = word

        if type is None:
            type = response.css('.bui-breadcrumb__text a.bui_breadcrumb__link::text').getall()[2]
            type_match = re.search(r'All\s+(.+)', type)
            type = (type_match.group(1) if type_match else 'Hotel').capitalize()
            

        review_count = int(response.xpath('//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[2]/div[2]/text()').get().split()[-2].replace(',', ''))

        price = response.xpath('//*[@id="hprt-table"]/tbody/tr[1]/td[3]/div/div/div[1]/div[2]/div/span/text()').get()
        if (price):
            price = int(re.search(r'\d+', price).group())

        print(title, '\n', description, '\n', star, '\n', address, '\n', city, '\n', location, '\n', score, '\n', price, '\n', type, '\n', review_count)
        # print(title, '\n', star, '\n', link, '\n', address, '\n', city, '\n', location, '\n', score, '\n', str(images), '\n', price, '\n', type, '\n', review_count, '\n', booking_id)

        
        # sql = """
        #     UPDATE booking_data 
        #     SET title = %s, description = %s, star = %s, link = %s, address = %s, city = %s, location = %s, score = %s, images = %s, type = %s, review_count = %s
        #     WHERE id = %s
        # """
        # self.cursor.execute(sql, (
        #     title, description, star, link, address, city, location, score, str(images), type, review_count, booking_id
        # ))
        # self.connection.commit()

        sql = """
            UPDATE booking_data 
            SET title = %s, description = %s, star = %s, address = %s, city = %s,
            location = %s, score = %s, images = %s, price = %s, type = %s, review_count = %s
            WHERE id = %s
        """
        self.cursor.execute(sql, (
            title, description, star, address, city, location, score, str(images), price, type, review_count, booking_id
        ))
        self.connection.commit()



        # получение удобств отеля 
        # facilities_tag = None
        # facilities_tag = response.xpath('//*[@id="basiclayout"]/div[1]/div[2]/div/div/div/div/div/ul/li/div[2]/div')
        # if not facilities_tag:
        #     facilities_tag = response.xpath('//ul[@class="c807d72881 d1a624a1cc e10711a42e"]/li/div/div/div/span/div/span')   

        # facilities = {} 

        # # Обработка найденных элементов
        # for element in facilities_tag:
        #     # Извлечение текста из каждого элемента и вывод его в консоль
        #     selected_text = element.xpath('./text()').get()
        #     print(selected_text)

        #     # Проверка наличия текста в facilities_cache
        #     existing_id = self.facilities_cache.get(selected_text)
        #     if existing_id is not None:
        #         facilities[selected_text] = existing_id
        #         print(f"Existing ID for '{selected_text}': {existing_id}")
        #     else:
        #         # If the selected_text is not found, add it to the database and cache
        #         self.cursor.execute('INSERT INTO facilities (title) VALUES (%s)', (selected_text,))
        #         self.connection.commit()
        #         new_id = self.cursor.lastrowid
        #         print(f"New ID for '{new_id}': {selected_text}")
        #         facilities[selected_text] = new_id
        #         self.facilities_cache[selected_text] = new_id

        # if facilities is not None:
        #     values_to_insert = [(booking_id, facility_id) for facility_name, facility_id in facilities.items()]
        #     print(values_to_insert)

        #     self.cursor.execute('DELETE FROM booking_facilities WHERE booking_id = %s', (booking_id,))
        #     self.connection.commit()

        #     self.cursor.executemany('INSERT INTO booking_facilities (booking_id, facilities_id) VALUES (%s, %s)', values_to_insert)
        #     self.connection.commit()

        # print(facilities)
        # print(self.facilities_cache)
