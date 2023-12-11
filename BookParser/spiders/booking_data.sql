-- Active: 1694892122116@@127.0.0.1@3306@test

DROP TABLE links;

CREATE TABLE IF NOT EXISTS links (
    id INT AUTO_INCREMENT PRIMARY KEY,
    link TEXT,
    UNIQUE (link(255)) 
)


DROP TABLE booking_data;

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
    FOREIGN KEY (booking_id) REFERENCES booking_data(id) ON DELETE CASCADE,
    title VARCHAR(255),
    max_people INT,
    prices VARCHAR(255),
    max_available_rooms INT,
    checkin VARCHAR(20),
    checkout VARCHAR(20),
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



ALTER TABLE booking_data ADD processed BOOLEAN DEFAULT 0;
CREATE EVENT reset_procced
ON SCHEDULE EVERY 1 DAY
STARTS TIMESTAMP(CURRENT_DATE, '07:00:00')
DO
BEGIN
    UPDATE booking_data SET procced = 0;
END;
