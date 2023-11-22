import re
import json
import scrapy
import mysql.connector
from urllib.parse import urlparse, parse_qs


class MySpider(scrapy.Spider):
    name = "booking"
    allowed_domains = ["www.booking.com"]
    start_urls = []
    connection = None
    
    def connect_to_db(self):
        config = {
            'user': 'root',
            'password': '1234',
            'host': 'localhost',
            'database': 'parser_booking',
            'raise_on_warnings': True
        }
        
        try:
            cnx = mysql.connector.connect(**config)
            return cnx
        except mysql.connector.Error as err:
            print(f"Ошибка подключения к базе данных: {err}")


    def start_requests(self):
        self.connection = self.connect_to_db()
        if self.connection and self.connection.is_connected():
            print('\nConnection to DB success\n')
        else:
            print('\nFailed to connect to DB. Exiting...\n')
            raise SystemExit("Failed to connect to DB")

        with open('booking_links.txt', 'r') as file:
            links = file.readlines()
            self.start_urls = [link.strip() for link in links]

        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'original_url': url})

    def parse(self, response):
        
        title = response.css('.d2fee87262.pp-header__title::text').extract_first()
        description = response.css('.a53cbfa6de.b3efd73f69::text').extract_first().strip()

        address = response.css('span.hp_address_subtitle::text').get().strip()
        coordinates = response.css('span.hp_address_subtitle::attr(data-bbox)').get()

        images = response.css('a.bh-photo-grid-item.bh-photo-grid-thumb > img::attr(src)').extract()

        rooms = []
        names = []
        max_people = []
        prices = []
        max_available_rooms = []
        rowspan = None
        
        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        for i in range(len(rows)):
            rowspan = rows[i].xpath('./td/@rowspan').get()
            if rowspan:
                room = {
                    'name': '',
                    'max_people': [],
                    'prices': [],
                    'max_available_rooms': [],
                }
                names.append(rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip())  
                room['name'] = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()

                for i in range(int(rowspan)):
                    max_people.append(int(rows[i].xpath('.//span[@class="bui-u-sr-only"]/text()').get().split(':')[-1].strip()))
                    prices.append(int(re.sub(r'[^\d.]', '', rows[i].xpath('.//span[@class="prco-valign-middle-helper"]/text()').get())))
                    max_available_rooms.append(rows[i].xpath('(//select[@class="hprt-nos-select js-hprt-nos-select"]//option)[last()]/@value').get())
                    
                    room['max_people'].append(int(rows[i].xpath('.//span[@class="bui-u-sr-only"]/text()').get().split(':')[-1].strip()))
                    room['prices'].append(int(re.sub(r'[^\d.]', '', rows[i].xpath('.//span[@class="prco-valign-middle-helper"]/text()').get())))
                    room['max_available_rooms'].append(rows[i].xpath('(//select[@class="hprt-nos-select js-hprt-nos-select"]//option)[last()]/@value').get())

            rooms.append(room)


        checkin = None
        checkout = None

        link = response.meta.get('original_url')

        url = urlparse(link)
        query_parameters = parse_qs(url.query)

        checkin = query_parameters.get('checkin')[0]
        checkout = query_parameters.get('checkout')[0]

        # Здесь вы можете использовать полученные данные для записи в базу данных
        with open('data.txt', 'a', encoding='utf-8') as f:
            # f.write(f"Title:\n{title}\n\nDescription:\n{description}\n\nAdress:\n{address}\n\nCoordinates:\n{coordinates}\n\n{images}\n\n")
            f.write(f"{json.dumps(rooms)}\n{link}\n{names}\n{max_people}\n{prices}\n{max_available_rooms}\n{checkin}\n\n")
            # f.write(f"{link}\n{json.dumps(rooms)}\n\n\n")


        cursor = self.connection.cursor()
        sql = """
            INSERT INTO booking_data 
            (title, description, link, address, coordinates, images, rooms, checkin, checkout)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            title, description, link, address, coordinates, str(images), json.dumps(rooms), checkin, checkout
        ))
        self.connection.commit()
