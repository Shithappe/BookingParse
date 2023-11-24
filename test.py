import mysql.connector

def connect_to_db():
    try:
        config = {
            'user': 'root',
            'password': '1234',
            'host': 'localhost',
            'database': 'parser_booking',
            'raise_on_warnings': True
        }

        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            print('Успешно подключено к базе данных')
            return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения к базе данных: {err}")
        return None

# Пример использования:
connection = connect_to_db()
if connection:
    # Выполнение запросов или других операций с базой данных
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM booking_data")
    # ...дополнительные операции с базой данных...
    connection.close()  # Важно закрыть соединение после использования
