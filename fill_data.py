import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from tabulate import tabulate
from datetime import datetime, timedelta
import random
from decimal import Decimal
from tqdm import tqdm  # Импортируем tqdm для прогресс-бара

def connect_to_db():
    """
    Подключение к базе данных с использованием параметров из переменных окружения.

    Возвращает:
        connection (mysql.connector.connection_cext.CMySQLConnection): Объект соединения.
        cursor (mysql.connector.cursor_cext.CMySQLCursorDict): Объект курсора с словарным выводом.
    """
    # Загрузка переменных окружения из .env файла
    load_dotenv()

    config = {
        'user': os.getenv('DATABASE_USER'),
        'password': os.getenv('DATABASE_PASSWORD'),
        'host': os.getenv('DATABASE_HOST'),
        'database': os.getenv('DATABASE_NAME'),
        'raise_on_warnings': True
    }

    try:
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            print("Успешно подключено к базе данных")
            cursor = connection.cursor(dictionary=True)
            return connection, cursor
    except Error as err:
        print(f"Ошибка подключения к базе данных: {err}")
        return None, None

def generate_available_rooms(avg_available, variation=0):
    """
    Генерирует значение available_rooms на основе среднего значения с добавлением разброса.

    Args:
        avg_available (float): Среднее значение доступных комнат.
        variation (float): Максимальное отклонение от среднего.

    Returns:
        int: Сгенерированное количество доступных комнат.
    """
    if variation > 0:
        # Добавляем случайное отклонение в пределах +/- variation
        available = avg_available + random.uniform(-variation, variation)
    else:
        # Если variation = 0, доступные комнаты равны среднему
        available = avg_available
    # Округляем до ближайшего целого числа и убеждаемся, что значение не отрицательное
    return max(int(round(available)), 0)

def generate_date_range(start_date_str, end_date_str):
    """
    Генерирует список дат между start_date и end_date включительно.

    Args:
        start_date_str (str): Начальная дата в формате 'YYYY-MM-DD'.
        end_date_str (str): Конечная дата в формате 'YYYY-MM-DD'.

    Returns:
        list of datetime.date: Список дат.
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    delta = end_date - start_date
    return [start_date + timedelta(days=i) for i in range(delta.days + 1)]

def print_table(data, headers="keys"):
    """
    Печатает данные в виде таблицы.

    Args:
        data (list of dict): Данные для отображения.
        headers (str или list): Заголовки таблицы или ключевое слово.
    """
    print(tabulate(data, headers=headers, tablefmt="pretty", showindex=False))

def main():
    # Период для заполнения
    start_date = '2024-09-20'
    end_date = '2024-09-26'

    # Генерация списка дат
    date_list = generate_date_range(start_date, end_date)

    # Подключение к базе данных
    conn, cursor = connect_to_db()
    if not conn or not cursor:
        return  # Завершить выполнение, если подключение не удалось

    query_rooms = """
        SELECT 
            room_id, 
            room_type,
            AVG(available_rooms) AS avg_available_rooms,
            MAX(available_rooms) - MIN(available_rooms) AS variation
        FROM rooms_2_day
        WHERE room_id IS NOT NULL AND room_id != 0
            AND checkin BETWEEN NOW() - INTERVAL 1 MONTH AND NOW()
        GROUP BY room_id 
        LIMIT 5
    """
    try:
        cursor.execute(query_rooms)
        rooms_data = cursor.fetchall()
        print("Полученные комнаты и их статистика available_rooms:")
        print(rooms_data)
    except Error as err:
        print(f"Ошибка выполнения запроса на получение room_id и статистики available_rooms: {err}")
        conn.close()
        return

    # Подготовка данных для вставки
    insert_data = []

    total_rooms = len(rooms_data)
    if total_rooms == 0:
        print("Нет данных о комнатах для обработки.")
        conn.close()
        return

    # Используем tqdm для отображения прогресса по комнатам
    for room in tqdm(rooms_data, desc="Progress", unit="unit"):
        room_id = room.get('room_id')
        room_type = room.get('room_type')
        avg_available = room.get('avg_available_rooms')
        variation = room.get('variation')

        if room_id is None or avg_available is None:
            print(f"Пропуск room_id={room_id} из-за отсутствия данных.")
            continue

        # Преобразование Decimal в float
        try:
            avg_available_float = float(avg_available)
            variation_float = float(variation) if variation is not None else 0.0
        except (TypeError, ValueError) as e:
            print(f"Ошибка преобразования статистики для room_id={room_id}: {e}")
            continue

        # Генерируем записи для каждого дня
        room_records = []
        for single_date in date_list:
            record = {
                'room_id': room_id,
                'room_type': room_type,
                'checkin': single_date.strftime('%Y-%m-%d'),
                'checkout': (single_date + timedelta(days=1)).strftime('%Y-%m-%d'),
                'available_rooms': generate_available_rooms(avg_available_float, variation_float)
            }
            room_records.append(record)

        # Вывод сгенерированных данных в виде таблицы
        # print(f"\n{room_id}")
        # print_table(room_records)
        tqdm.write(f"\n{room_id}")
        tqdm.write(tabulate(room_records, headers="keys", tablefmt="pretty", showindex=False))

        # Добавляем записи в общий список для вставки
        # insert_data.extend(room_records)

    # Вставка данных в базу данных
    # if insert_data:
    #     insert_query = """
    #         INSERT INTO rooms_2_day (room_id, checkin, checkout, available_rooms)
    #         VALUES (%s, %s, %s, %s)
    #         ON DUPLICATE KEY UPDATE
    #             available_rooms = VALUES(available_rooms)
    #     """
    #     insert_values = [
    #         (record['room_id'], record['checkin'], record['checkout'], record['available_rooms'])
    #         for record in insert_data
    #     ]
    #     try:
    #         cursor.executemany(insert_query, insert_values)
    #         conn.commit()
    #         print("\nДанные успешно вставлены/обновлены в базе данных.")
    #     except Error as err:
    #         print(f"Ошибка вставки данных в базу данных: {err}")
    #         conn.rollback()
    # else:
    #     print("Нет данных для вставки.")

    # Закрытие соединения
    cursor.close()
    conn.close()
    print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()



# % выполнения
# вставка в таблицу блоками

    # query_rooms = """
    #     SELECT 
    #         room_id, 
    #         AVG(available_rooms) AS avg_available_rooms
    #     FROM rooms_2_day
    #     WHERE room_id IS NOT NULL AND room_id != 0 and room_type = 'Two-Bedroom Villa' and booking_id = 9011
    #         AND checkin BETWEEN NOW() - INTERVAL 1 MONTH AND NOW()
    #     GROUP BY room_id 
    #     LIMIT 10
    # """

# 1) получить список всех комнат
# 2) от даты начала с диапазоном в месяц получить среднее значение для комнаты
# 3) добавить небольшую погрешность и сформировать данные для вставки 
#     погрешность должна завистеть от макс значение комнаты 