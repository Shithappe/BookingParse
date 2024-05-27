-- Active: 1701476604929@@artnmo.mysql.tools@3306@artnmo_estate

DROP TABLE links;

CREATE TABLE IF NOT EXISTS links (
    id INT AUTO_INCREMENT PRIMARY KEY,
    link TEXT,
    UNIQUE (link(255)) 
)


-- DROP TABLE booking_data;

CREATE TABLE IF NOT EXISTS booking_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    star INT,
    link TEXT, UNIQUE (link(255)), 
    address VARCHAR(255),
    city VARCHAR(255),
    coordinates VARCHAR(255),
    images TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)


DROP TABLE rooms;

CREATE TABLE IF NOT EXISTS rooms (
    id INT AUTO_INCREMENT PRIMARY KEY,
    booking_id INT,
    -- FOREIGN KEY (booking_id) REFERENCES booking_data(id) ON DELETE CASCADE,
    room_type VARCHAR(255),
    max_available INT,
    active BOOLEAN DEFAULT False,
    price INT,
    occupancy INT,
    UNIQUE KEY uniq_booking_room (booking_id, room_type),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)


DROP TABLE rooms_2_day; 

CREATE TABLE IF NOT EXISTS rooms_2_day ( 
    id INT AUTO_INCREMENT PRIMARY KEY, 
    booking_id INT,
    FOREIGN KEY (booking_id) REFERENCES booking_data(id) ON DELETE CASCADE,
    room_type VARCHAR(255),
    max_available_rooms INT DEFAULT 0,
    checkin VARCHAR(20),
    checkout VARCHAR(20), 
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)


DROP TABLE rooms_30_day; 

CREATE TABLE IF NOT EXISTS rooms_30_day (
    id INT AUTO_INCREMENT PRIMARY KEY,
    booking_id INT,
    FOREIGN KEY (booking_id) REFERENCES booking_data(id) ON DELETE CASCADE,
    room_type VARCHAR(255),
    max_available_rooms INT,
    checkin VARCHAR(20),
    checkout VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- UNIQUE (booking_id, checkin, checkout, created_at)
)

DROP TABLE remaining_rooms; 

CREATE TABLE IF NOT EXISTS remaining_rooms (
    id INT AUTO_INCREMENT PRIMARY KEY,
    booking_id INT,
    FOREIGN KEY (booking_id) REFERENCES booking_data(id) ON DELETE CASCADE,
    room_type VARCHAR(255),
    available_rooms INT,
    checkin VARCHAR(20),
    checkout VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (booking_id, room_type, checkin, checkout)
)



ALTER TABLE booking_data ADD processed BOOLEAN DEFAULT 0;
CREATE EVENT reset_procced
ON SCHEDULE EVERY 1 DAY
STARTS TIMESTAMP(CURRENT_DATE, '07:00:00')
DO
BEGIN
    UPDATE booking_data SET procced = 0;
END;



DROP TABLE room_cache; 

CREATE TABLE IF NOT EXISTS room_cache (
    id INT AUTO_INCREMENT PRIMARY KEY,
    booking_id INT,
    FOREIGN KEY (booking_id) REFERENCES booking_data(id) ON DELETE CASCADE,
    room_type VARCHAR(255),
    max_available INT,
    occupancy_rate FLOAT
)


DROP TABLE facilities; 

CREATE TABLE IF NOT EXISTS facilities (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    UNIQUE (title)
)


CREATE TABLE IF NOT EXISTS facilities (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255)
)


DROP TABLE booking_facilities; 

CREATE TABLE IF NOT EXISTS booking_facilities (
    booking_id INT,
    facilities_id INT,
    UNIQUE (booking_id, facilities_id)
)

