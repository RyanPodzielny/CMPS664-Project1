DROP DATABASE IF EXISTS flightsDB;
CREATE DATABASE flightsDB;
USE flightsDB;

CREATE TABLE flights (
    src VARCHAR(10) NOT NULL,
    dest VARCHAR(10) NOT NULL,
    travel_date DATE NOT NULL,
    flight_id INT PRIMARY KEY,
    seats_left INT NOT NULL DEFAULT 300,
    first INT NOT NULL DEFAULT 50,
    business INT NOT NULL DEFAULT 100,
    economy INT NOT NULL DEFAULT 150
);

CREATE TABLE bookings (
	booking_id INT PRIMARY KEY auto_increment,
	first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    age INT,
    src VARCHAR(10) NOT NULL,
    dest VARCHAR(10) NOT NULL,
    travel_date DATE NOT NULL,
    class VARCHAR(255) NOT NULL,
    booking_time TIME NOT NULL,
    npass INT NOT NULL,
    flight_id INT NOT NULL,
    FOREIGN KEY (flight_id) REFERENCES flights(flight_id)
);

SELECT * FROM flights;
SELECT * FROM bookings;

-- Example Queries

-- Query 1: Show the flight schedule between two airports between two dates
-- Includes both ways between the airports
SELECT * FROM flights
WHERE (src = 'JFK' AND dest = 'DEN') OR (src = 'DEN' AND dest = 'JFK')
	AND travel_date BETWEEN '2100-01-01' AND '2100-01-07';
    
-- Query 2: Rank top 3 {source, destination, booking_requests} airports based on the booking requests for a week
-- Only showing top 3, if there is a tie so be it
SELECT src, dest, COUNT(*) AS booking_requests FROM bookings
	WHERE travel_date BETWEEN '2100-01-01' AND '2100-01-08'
	GROUP BY src, dest
	ORDER BY booking_requests DESC
	LIMIT 3;
    
-- Query 3: Next available (has seats) flight between given airports
-- Specific src to dest, not both
SELECT * FROM flights
    WHERE src = 'JFK' AND dest = 'DEN' 
    AND travel_date >= CURDATE() AND seats_left > 0
    ORDER BY travel_date
    LIMIT 1;

-- Query 4: Average occupancy rate (%full) for all flights between two cities
-- Gets average val of both way flights, makes more sense that way
SELECT src, dest, AVG((300 - seats_left) / 300.0 * 100) AS average_occupancy_rate
	FROM flights
	WHERE (src= 'JFK' AND dest = 'DEN') OR (src = 'DEN' AND dest = 'JFK')
	GROUP BY src, dest;