import requests
import mysql.connector
from mysql.connector import Error

# Установите параметры для подключения к базе данных MySQL
db_config = {
            'user': 'artnmo_estate',
            'password': 'gL8+8uBs2_',
            'host': 'artnmo.mysql.tools',
            'database': 'artnmo_estate',
            'raise_on_warnings': True
        }

url = "https://booking-com.p.rapidapi.com/v1/hotels/search"

headers = {
    # "x-rapidapi-key": "8055d9b3d1msh4deb132b9d59203p1421c4jsnd67afd362fe3",
    "x-rapidapi-key": "ad46798c5dmsh0726e0c9074aa41p1f7047jsn9dd64c61c0ab",
    "x-rapidapi-host": "booking-com.p.rapidapi.com"
}

def format_link(original_link):
    return original_link.replace('.html', '.en-gb.html')

def extract_hotel_details(data):
    if 'result' in data:
        return [
            {
                'title': item.get('hotel_name_trans'),
                'link': format_link(item.get('url')),
                'country': 'Spain'
            }
            for item in data['result']
        ]
    else:
        return []

def insert_hotel_details(hotel_details, connection, cursor):
    try:
        # SQL запрос для вставки данных
        insert_query = """
        INSERT INTO booking_data (title, link, country) 
        VALUES (%s, %s, %s)
        """

        for hotel in hotel_details:
            cursor.execute(insert_query, (hotel['title'], hotel['link'], hotel['country']))

        connection.commit()
        print(f"{cursor.rowcount} records inserted successfully.")

    except Error as e:
        print(f"Error: {e}")

# with open('regions.txt', 'r') as file:
#     lines = file.readlines()
# region_ids = [line.strip() for line in lines]


try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # for region_id in region_ids:
    for page in range(1, 60):
        querystring = {
            "checkout_date": "2024-09-15",
            "order_by": "popularity",
            "filter_by_currency": "AED",
            "include_adjacency": "true",
            "room_number": "1",
            "dest_id": f"{777}",
            "dest_type": "region",
            "adults_number": "2",
            "page_number": f"{page}",
            "checkin_date": "2024-09-14",
            "locale": "en-gb",
            "units": "metric"
        }
        response = requests.get(url, headers=headers, params=querystring)

        hotel_details = extract_hotel_details(response.json())
        insert_hotel_details(hotel_details, connection, cursor)

except Error as e:
    print(f"Error: {e}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")



