import mysql.connector

def connect_to_db():
    # Параметры подключения к базе данных
    config = {
        'user': 'root',
        'password': '1234',
        'host': 'localhost',
        'database': 'parser_booking',
        'raise_on_warnings': True
    }

    # Установка подключения
    try:
        cnx = mysql.connector.connect(**config)
        return cnx
    except mysql.connector.Error as err:
        print(f"Ошибка подключения к базе данных: {err}")