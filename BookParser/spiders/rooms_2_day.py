import scrapy
import mysql.connector
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse



class UpdateRoomsSpider(scrapy.Spider):

    # mod = 0  # четные  # estate 1
    mod = 1  # не четные  # ai-pdf 2
    

    today = datetime.now().date()
    # today = today + timedelta(days=2)  # for debug

    checkin = today + timedelta(hours=8)
    checkout = checkin + timedelta(days=1)    

    name = "rooms_2_day"
    allowed_domains = ["www.booking.com"]
    start_urls = ["https://www.booking.com"]
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

    def set_to_db(self, booking_id, value, checkin, checkout):
        print('\nWRITE TO DB\n')
        print(booking_id, value, checkin, checkout)
        # try:
        #     self.cursor.executemany("""
        #         INSERT INTO rooms_2_day
        #         (booking_id, room_type, available_rooms, price, checkin, checkout)
        #         VALUES (%s, %s, %s, %s, %s, %s)
        #     """, [(booking_id, room_type, count, price, checkin, checkout) for room_type, count, price in value])

        #     self.connection.commit()
        #     # print("Insert successful")
        # except Exception as e:
        #     print(f"Error: {e}")

        

    def start_requests(self):
        self.connection = self.connect_to_db()
        if self.connection and self.connection.is_connected():
            print('\nConnection to DB success\n')
        else:
            print('\nFailed to connect to DB. Exiting...\n')
            raise SystemExit("Failed to connect to DB")
        
        self.cursor = self.connection.cursor()

        rows = None

         # последий обработыный booking_id за сегодня для этого парсера
        self.cursor.execute(f"SELECT MAX(booking_id) FROM rooms_2_day WHERE created_at >= '{self.today}' AND booking_id MOD 2 = {self.mod}")
        rows = self.cursor.fetchall()

        if rows[0][0]:
            # удалить все записи за сегодня с последним booking_id (т.к. он может быть не полным)
            print(f'Continuing with booking_id {rows[0][0]}')
            self.cursor.execute(f"DELETE FROM rooms_30_day WHERE created_at = '{self.today}' AND booking_id = {rows[0][0]}")
            self.cursor.fetchall()

            # продолжить обход записей с предпоследнего значения 
            self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id >= {rows[0][0]} AND id MOD 2 = {self.mod}')
            rows = self.cursor.fetchall()
        else:
            # в слуае отсутствия записей за сегодня для этого парсера  
            self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id MOD 2 = {self.mod}')
            rows = self.cursor.fetchall()

        
        # self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id = 2017')
        # rows = self.cursor.fetchall()

        for row in rows:
                formatted_link = self.format_link(row[1], self.checkin, self.checkout) 
                request_meta = {
                    'booking_id': row[0],
                    'link': row[1],
                    'checkin': self.checkin, 
                    'checkout': self.checkout
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)

    def parse(self, response):

        booking_id = response.meta.get('booking_id')

        self.cursor.execute(f'''SELECT room_type, max_available
                            FROM rooms
                            WHERE booking_id = {booking_id}
                            GROUP BY room_type''')
        max_available = self.cursor.fetchall()

        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')        

        room_type = None
        available_rooms = None
        rowspan = None

        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        room_types_count = {}
        room_prices = {}

        if rows:
            for i in range(len(rows)):
                rowspan = rows[i].xpath('./td/@rowspan').get()
                if rowspan:
                    room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()
                    available_rooms = rows[i].xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get()                
                    price = response.xpath(f'//*[@id="hprt-table"]/tbody/tr[{i+1}]/td[3]/div/div/div[1]/div[2]/div/span/text()').get()
                    if price:
                        price = ''.join(filter(str.isdigit, price))
                    
                    print(f'{room_type}: {available_rooms} {price}')

                    if not available_rooms: 
                        available_rooms = 0

                    count = int(available_rooms)

                    if room_type in room_types_count:
                        room_types_count[room_type] += count
                    else:
                        room_types_count[room_type] = count

                    if room_type not in room_prices:
                        room_prices[room_type] = price

                    for max_type, max_count in max_available:
                        if max_type == room_type:
                            max_available.remove((max_type, max_count))
                            break

            # Устанавливаем значение 0 для элементов, которые остались в max_available
            max_available = [(max_type, 0) for max_type, max_count in max_available]

            # Объединяем room_types_count, max_available и добавляем цены
            combined_rooms = []
            for room_type, count in room_types_count.items():
                combined_rooms.append((room_type, count, room_prices.get(room_type)))

            for max_type, _ in max_available:
                combined_rooms.append((max_type, 0, room_prices.get(max_type)))

            # print(combined_rooms)

            self.set_to_db(booking_id, combined_rooms, checkin, checkout)


        else:
            alert_title = response.css('.bui-alert__title::text').get()
            print(alert_title)

            if 'is a minimum length of stay of' in alert_title:
                book_size = int(alert_title.split(' ')[-2])
                # print(book_size)


                checkin = self.today + timedelta(hours=8)
                checkout = checkin + timedelta(days=book_size)


                formatted_link = self.format_link(response.meta.get('link'), checkin, checkout) 
                # print('formatted_link')
                # print(formatted_link)

                request_meta = {
                    'booking_id': booking_id,
                    'link': response.meta.get('link'),
                    'checkin': checkin, 
                    'checkout': checkout
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)


            else:
                print('set 0 avalible')
                print('max_available: ', max_available)
                updated_list = [(name, 0, None) for name, _ in max_available]
                print('updated_list: ', updated_list)

                self.set_to_db(booking_id, updated_list, checkin, checkout)

