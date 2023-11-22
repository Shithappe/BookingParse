-- Active: 1694892122116@@127.0.0.1@3306@parser_booking

DROP TABLE booking_data;

CREATE TABLE IF NOT EXISTS booking_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    link TEXT,
    address VARCHAR(255),
    coordinates VARCHAR(255),
    images TEXT,
    rooms JSON,
    -- names TEXT,
    -- max_people TEXT,
    -- prices TEXT,
    -- max_available_rooms TEXT,
    checkin VARCHAR(20),
    checkout VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
