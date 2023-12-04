import scrapy
import mysql.connector
from urllib.parse import quote
# from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

class BookingSpider(scrapy.Spider):

    name = "get_links"
    allowed_domains = ["www.booking.com"]
    start_urls = []  

    count_pages = 1000
    connection = None
    cursor = None
    sql = """
            INSERT IGNORE INTO links 
            (link)
            VALUES (%s)
        """
    
    main_url = 'https://www.booking.com/searchresults.en-gb.html?ss=Bali%2C+Indonesia&lang=en-gb&dest_type=region&search_selected=true&group_adults=1&no_rooms=1&group_children=0&nflt='


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

    def start_requests(self):
        self.connection = self.connect_to_db()
        if self.connection and self.connection.is_connected():
            self.cursor = self.connection.cursor()
            print('\nConnection to DB success\n')
        else:
            raise SystemExit("Failed to connect to DB")
        
        with open('query_list.txt', 'r') as file:
            lines = file.readlines()
            self.start_urls = [self.main_url + quote(line.strip()) for line in lines]

        for start_url in self.start_urls:
            for i in range(0, self.count_pages, 25):  
                count_item = i + 1
                next_page_url = f"{start_url}&offset={count_item}"
                yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse(self, response):
        for a_tag in response.css('a[data-testid="title-link"]'):
            link = a_tag.css('::attr(href)').extract_first().split('?')[0]

            self.cursor.execute(self.sql, [link])
            self.connection.commit()
