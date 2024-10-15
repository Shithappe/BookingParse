import os
import mysql.connector

# Подключение к базе данных
def connect_to_db():
    return mysql.connector.connect(
        host="artnmo.mysql.tools",     # или другой адрес хоста
        user="artnmo_estate", # замените на имя пользователя
        password="gL8+8uBs2_", # замените на ваш пароль
        database="artnmo_estate"  # замените на вашу базу данных
    )

def fetch_data(cursor):
    query = """
        SELECT 
            ri.room_id AS room_id,
            ri.booking_id AS booking_id,
            ri.room_type AS room_type
        FROM 
            rooms_id ri
        JOIN 
            rooms_2_day r2d
        ON 
            ri.booking_id = r2d.booking_id
            AND ri.room_type = r2d.room_type
        WHERE r2d.room_id is NULL AND checkin BETWEEN '2024-08-01' AND '2024-10-10'
        GROUP BY
            ri.room_id
    """
    cursor.execute(query)
    return cursor.fetchall()

def update_rooms_2_day(cursor, room_id, booking_id, room_type):
    query = """
        UPDATE rooms_2_day 
        SET room_id = %s 
        WHERE booking_id = %s 
          AND room_type = %s
    """
    cursor.execute(query, (room_id, booking_id, room_type))

def main():
    conn = connect_to_db()
    cursor = conn.cursor()
    
    rooms_data = fetch_data(cursor)
    rooms_len = len(rooms_data)

    for index, item in enumerate(rooms_data):
        if (index + 1) % 10 == 0:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f'{round((index + 1) / rooms_len * 100, 2)}%')
        # print(f'{round(index / rooms_len * 100, 2)}%')
        room_id, booking_id, room_type = item
        
        # print(room_id, booking_id, room_type)
        # print(cursor.rowcount)
        update_rooms_2_day(cursor, room_id, booking_id, room_type)
    

    conn.commit()
    cursor.close()
    conn.close()
    print('\nDone', rooms_len)

if __name__ == "__main__":
    main()