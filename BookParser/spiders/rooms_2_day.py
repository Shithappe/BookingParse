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
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            return connection, cursor

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
        print(value)
        try:
            formatted_values = [
                (booking_id, item['room_id'], item['room_type'], item['available_rooms'], item['price'], checkin, checkout) for item in value
            ]

            # Выполняем вставку
            self.cursor.executemany("""
                INSERT INTO rooms_2_day
                (booking_id, room_id, room_type, available_rooms, price, checkin, checkout)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, formatted_values)

            self.connection.commit()
            print("Insert successful")
        except Exception as e:
            print(f"DB Error: {e}")

        

    def start_requests(self):
        self.connection, self.cursor = self.connect_to_db()

        rows = None

         # последий обработыный booking_id за сегодня для этого парсера
        self.cursor.execute(f"SELECT MAX(booking_id) FROM rooms_2_day WHERE created_at >= '{self.today}' AND booking_id MOD 2 = {self.mod}")
        rows = self.cursor.fetchall()

        if rows[0][0]:
            # удалить все записи за сегодня с последним booking_id (т.к. он может быть не полным)
            print(f'Continuing with booking_id {rows[0][0]}')
            self.cursor.execute(f"DELETE FROM rooms_2_day WHERE created_at = '{self.today}' AND booking_id = {rows[0][0]}")
            self.cursor.fetchall()

            # продолжить обход записей с предпоследнего значения 
            self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id >= {rows[0][0]} AND id MOD 2 = {self.mod}')
            rows = self.cursor.fetchall()
        else:
            # в слуае отсутствия записей за сегодня для этого парсера  
            self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id MOD 2 = {self.mod}')
            rows = self.cursor.fetchall()

        
        # self.cursor.execute(f'SELECT id, link FROM booking_data WHERE id = 8978')
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

        self.cursor.execute(f'''SELECT room_id
                                FROM rooms_id
                                WHERE booking_id = {booking_id}
                                GROUP BY room_id''')
        ext_rooms_id = self.cursor.fetchall()
        room_ids = [room_id[0] for room_id in ext_rooms_id]

        print(f'\n{room_ids}\n')

        checkin = response.meta.get('checkin')
        checkout = response.meta.get('checkout')        

        rows = response.xpath('//*[@id="hprt-table"]/tbody/tr')

        rooms_data = []

        if rows:
            for i in range(len(rows)):
                rowspan = rows[i].xpath('./td/@rowspan').get()
                if rowspan:
                    room_id = rows[i].xpath('.//a[@class="hprt-roomtype-link"]/@data-room-id').get()
                    room_type = rows[i].xpath('.//span[contains(@class, "hprt-roomtype-icon-link")]/text()').get().strip()
                    available_rooms = rows[i].xpath('.//select[@class="hprt-nos-select js-hprt-nos-select"]//option[last()]/@value').get()                
                    price = rows[i].xpath(f'./td[3]/div/div/div[1]/div[2]/div/span/text()').get()
                    if price:
                        price = ''.join(filter(str.isdigit, price))
                    if not available_rooms: 
                        available_rooms = 0
                    available_rooms = int(available_rooms)

                    rooms_data.append({
                        'room_id': room_id,
                        'room_type': room_type,
                        'available_rooms': available_rooms,
                        'price': price
                    })

                    # print(f'{room_id} {room_type}: {available_rooms} {price}')



            existing_room_ids = [int(room['room_id']) for room in rooms_data]

            for room_id in room_ids:
                if room_id not in existing_room_ids:
                    rooms_data.append({
                        'room_id': room_id,
                        'room_type': None,
                        'available_rooms': 0,
                        'price': None
                    })
            for room in rooms_data:
                print(f"{room['room_id']} {room['room_type']}: {room['available_rooms']} {room['price']}")

            # self.set_to_db(booking_id, combined_rooms, checkin, checkout)
            # нужно тоже самое сделать для update.. и записать в бд


        else:
            alert_title = response.css('.bui-alert__title::text').get()
            print(alert_title)

            if 'is a minimum length of stay of' in alert_title:
                book_size = int(alert_title.split(' ')[-2])
                # print(book_size)


                checkin = self.today + timedelta(hours=8)
                checkout = checkin + timedelta(days=book_size)


                formatted_link = self.format_link(response.meta.get('link'), checkin, checkout) 
                # print('formatted_link ', formatted_link)

                request_meta = {
                    'booking_id': booking_id,
                    'link': response.meta.get('link'),
                    'checkin': checkin, 
                    'checkout': checkout
                }
                yield scrapy.Request(url=formatted_link, callback=self.parse, meta=request_meta)


            else:
                print('set 0 avalible')

                for room_id in room_ids:
                    rooms_data.append({
                        'room_id': room_id,
                        'room_type': None,
                        'available_rooms': 0,
                        'price': None
                    })

                # print('max_available: ', max_available)
                # updated_list = [(room_id, room_type, 0, None) for room_id, room_type, max_count in max_available]
                # print('updated_list: ', updated_list)

                self.set_to_db(booking_id, rooms_data, checkin, checkout)

