# Project 1 - Part A

import mysql.connector
from collections import defaultdict
from lxml import etree # type: ignore


# Using a class here was just an easier way to divide the logic. We could simply
#   have all the functions outside of a class and just pass the db and cursor
#   around but it gets complicated and messy.
class Airline:
    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db

        """
        Flight is same source to dest and same travel date Max 300 people per
        flight, need to take into account npass

        Each travel unique travel date in the booking is considered a flight,
        regardless if it shares a source and destination with another flight. I
        am assuming that the travel date in the booking request is when the
        person booked the flight time, and when they are supposed to fly. This
        does result in a lot of flights, with minimal occupancy no flights reach
        their limit of 300 passengers, but as the data is already set as the
        travel date setting it to another date makes no sense. We could
        potentially change the travel data, but imagine someone books a flight
        for a certain data but we change it to something else randomly. 

        Stored as (id, seats_left, business, first, economy) so we can add more
        flights if needed and keep track of the id of the flight. We also need
        to keep track of the seats taken in each specific class as well, hence
        we also will be storing seats taken for each of those as well. Like
        this: {(source, dest, travelDate): [(id, seats_left, business, first,
        economy), ...]}
        """
        self.flights = defaultdict(dict)
        # Keeps track of next flight id to assign
        self.flight_id = 1


        """
        Stored as [[first_name, last_name, address, age, src, dest, travel_date,
        class, booking_time, npass, flight_id], ...]

        We include flight_id as it is important to know which flight the booking
        is on - though not needed for our usecase

        We use a list because we don't need to access the data by name, and it's
        easier to iterate through. Makes it easier to insert into database

        People represented as a list of lists, i.e. each booking in the xml
        """
        self.reservations = []


        """
        Constants to make it easier to change in the future if needed, and to 
        avoid hardcoding values throughout the code
        """
        # 300 seats per flight, with 3 classes
        self.CAPACITY = {"first": 50, "business": 100, "economy": 150}

        # Constant mappings for the XML parsing as its not fully structured
        self.FIELD_MAPPING = {
            0: "first_name",
            1: "last_name",
            2: "address",
            3: "age",
            4: "src",
            5: "dest",
            6: "travel_date",
            7: "class",
            8: "booking_time",
            9: "npass"
        }

        self.FLIGHT_COLUMNS = ["src", "dest", "travel_date", "flight_id", 
                               "seats_left", "first", "business", "economy"]
        self.RESERVATION_COLUMNS = ["first_name", "last_name", "address", "age", 
                                    "src","dest", "travel_date", "class", 
                                    "booking_time", "npass", "flight_id"]


    def parse_xml(self):
        # Parse the XML file
        root = etree.parse("PNR.xml")
        # Namespace for excel file - used to get data
        NAMESPACE = "{urn:schemas-microsoft-com:office:spreadsheet}"

        # Loop through all rows (data pointers) in excel file. The XML is not
        #   very well structured (no tags for each field) so we need to go line
        #   by line and keep track of which field we are on
        for row in root.iter(NAMESPACE + "Row"):
            # Don't record first row - headers
            if (row.attrib.get(NAMESPACE + "Index") == "1"):
                continue
            
            # We will define a dict here to make it easier to keep track by name
            #   instead of by index, converting to a list later on
            booking = {}

            # Get all data from row - need to convert age and npass to int. We
            #   essentially iterate through each row, and only get the data from
            #   that specific row. Makes it much easier to keep track of
            for index, cell in enumerate(row.iter(NAMESPACE + "Data")):
                value = cell.text
                # Convert age and npass to int
                if cell.attrib.get(NAMESPACE + "Type") == "Number":
                    value = int(cell.text)
                # Strip and replace commas with whiespace
                else:
                    value = cell.text.strip().replace(",", " ")


                # We use the mapping as key for the booking (easier to track)
                booking[self.FIELD_MAPPING[index]] = value

            # Only record if all fields are filled, we have 1 booking that
            #   doesn't include a last name so I am tossing it out. Hard to
            #   parse data with no tags for each field and no way to know what
            #   each field is, best to just ignore the data that is missing
            if len(booking) != len(self.FIELD_MAPPING):
                continue

            # We have a booking, so reserve a seat for it
            self.reserve_seat(booking)


    def create_flight(self, src, dest, travel_date):
        # Create one if it doesn't exist, otherwise get the flight
        key = (src, dest, travel_date)

        # Doesn't exist, so create based on the src, dest and travel date
        if key not in self.flights:
            new_flight = {
                "id": self.flight_id,
                "seats_left": sum(self.CAPACITY.values()),
                "business": self.CAPACITY["business"],
                "first": self.CAPACITY["first"],
                "economy": self.CAPACITY["economy"]
            }

            # Add it to the dict, and increase our flight id
            self.flights[key] = new_flight
            self.flight_id += 1
            return new_flight
        
        # Key exists, so just return the flight
        else:
            return self.flights[key]
        

    def reserve_seat(self, booking):
        # Can't accommodate booking, more passengers than total seats
        if booking["npass"] > sum(self.CAPACITY.values()):
            return False

        # Using a cascade for seat allocation. We want to try to allocate on the 
        #   class they booked, but if we can't accommodate them there we want to 
        #   try first upgrade to next class, then downgrade if possible.
        PREFERENCES = {
            "economy": ["economy", "business", "first"],
            "business": ["business", "first", "economy"],
            "first": ["first", "business", "economy"]
        }
        
        # Get the flight for the booking, creating it if it doesn't exist
        flight = self.create_flight(booking["src"], 
                                    booking["dest"], 
                                    booking["travel_date"])
        
        # In case we need to revert flight back to original state as we can't
        #   accommodate the booking
        roll_back_copy = flight.copy()
            
        # Get how many seats we need for the specific flight
        seats_needed = booking["npass"]

        # Now we try to allocate seats based on the preferences for the class,
        #   going to next class if we couldn't accommodate them in the preferred
        for class_type in PREFERENCES[booking["class"]]:
            # All seats were filed, we booked them on the flight
            if seats_needed == 0 or seats_needed > flight["seats_left"]:
                break
            
            # Fill the seats as much as we can
            if flight["seats_left"] > 0:
                # Can only fill as many seats as we have left, so min of them
                num_seats = min(seats_needed, flight[class_type])

                # Update our flight with the new seating arrangement
                flight[class_type] -= num_seats
                flight["seats_left"] -= num_seats

                # And reduce number of seats we need to fill
                seats_needed -= num_seats
        
        # We couldn't accommodate all npass - so fix flight back to original
        if seats_needed != 0:
            # Ensure we filled seats 
            if flight["seats_left"] != sum(self.CAPACITY.values()):
                flight = roll_back_copy
            return False
    
        # Now add the booking to the reservations with their flight id
        booking["flight_id"] = flight["id"]
        self.reservations.append(booking)

        return True
            

    def insert_bookings(self):
        # For all the reserverations we have, insert them into the booking table
        for booking in self.reservations:
            query = f"INSERT INTO bookings ("
            query += f"{(', ').join(self.RESERVATION_COLUMNS)})"
            query += " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            # Need to insert directly as .values() doesn't guarantee order
            self.cursor.execute(query, (booking["first_name"], 
                                        booking["last_name"], 
                                        booking["address"], 
                                        booking["age"], 
                                        booking["src"], booking["dest"], 
                                        booking["travel_date"], 
                                        booking["class"], 
                                        booking["booking_time"],
                                        booking["npass"],
                                        booking["flight_id"]))
            
        # And make sure to commit
        self.db.commit()


    def insert_flights(self):
        # For all the flights we have, insert them into the flight table
        for key, values in self.flights.items():
            query = "INSERT INTO flights ("
            query += f"{(', ').join(self.FLIGHT_COLUMNS)})"
            query += " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

            # Once again no gaurantee of order, so must insert directly
            self.cursor.execute(query, (key[0], key[1], key[2], 
                                        values["id"], 
                                        values["seats_left"],
                                        values["first"], 
                                        values["business"], 
                                        values["economy"]))
        self.db.commit()


    # Small check to see if the booking id exists (used for check in)
    def checkin(self, booking_id):
        self.cursor.execute("SELECT * FROM bookings WHERE booking_id = %s", 
                            (booking_id,))
        booking = self.cursor.fetchone()

        # If we don't have a booking with that id, return false, otherwise true
        if not booking:
            return False
        return True

    
    
# Helper function to write csvs and outputing example quesries. Makes it easier
#   for the output and to use the csv for output b
def write_to_file(message, file_name, data):
    with open(file_name, "w") as f:
        f.write(message + "\n")

        for row in data:
            # Need to convert to string to write and join
            values = [str(value) for value in row]
            f.write((",").join(values) + "\n")


# Output all the results to terminal - once again just easier this way
def read_from_file(file_name):
    with open(file_name, "r") as f:
        return f.read()


# Purely for Part B to showcase the normalization of the data. This is 
#   suboptimal form for the database, making it perfect to showcase the benefits
#   of normalization
def combined_csv(airline):
    with open("combined.csv", "w") as f:
        # Don't want to double count flight_id, src, dest and travel date so we 
        #   only include it once
        f.write("booking_id," + ",".join(airline.RESERVATION_COLUMNS[:-1]) +\
                "," + ",".join(airline.FLIGHT_COLUMNS[3:]) + "\n")
        
        # Simply enumerating to get the booking id (much easier than querying
        #   the database again)
        for id, booking in enumerate(airline.reservations, start=1):
            # Our flight all exist (we don't book without it) so we can just get
            #   the key based on the booking
            flight = airline.flights[(booking["src"], 
                                      booking["dest"], 
                                      booking["travel_date"])]
            
            # Need to convert to string to write and join, and ensuring we don't
            #   double count flight_id, src, dest and travel date
            values = [str(booking[col]) for col in airline.RESERVATION_COLUMNS[:-1]] 
            values += [str(value) for value in flight.values()]

            # Finally write to combined csv
            f.write(str(id) + "," + (",").join(values) + "\n")
        
        
# These are purely based on the project description examples. There is also a 
#   SQL file with the same queries, but running them here is more consistent for
#   outputting the results to a file and terminal
def example_queries(cursor):
    # Queries 
    # Done here, and in the SQL file, as we need to store the output to a file

    # Query 1
    q1 = "Query 1: Show the flight schedule between two airports between two dates\n"
    q1 += "Results from JFK and DEN between 2100-01-01 and 2100-01-07 {src, dest, date, flight_id, seats_left, first, business, economy}\n"

    sql = """
    SELECT * FROM flights
    WHERE (src = 'JFK' AND dest = 'DEN') OR (src = 'DEN' AND dest = 'JFK')
        AND travel_date BETWEEN '2100-01-01' AND '2100-01-07';
    """
    cursor.execute(sql)
    write_to_file(q1, "Project1PartA-Query1Output.txt", cursor)


    # Query 2
    q2 = "Query 2: Rank top 3 airports based on the booking requests for a week\n"
    q2 += "Results from 2100-01-01 to 2100-01-08 {src, dest, booking_request}\n"

    # We don't care about tie, only top 3
    sql = """
    SELECT src, dest, COUNT(*) AS booking_requests FROM bookings
        WHERE travel_date BETWEEN '2100-01-01' AND '2100-01-08'
        GROUP BY src, dest
        ORDER BY booking_requests DESC
        LIMIT 3;
    """
    cursor.execute(sql)
    write_to_file(q2, "Project1PartA-Query2Output.txt", cursor)


    # Query 3
    q3 = "Query 3: Next available (has seats) flight between given airports\n"
    q3 += "Results from JFK to DEN {flight_id, src, dest, date, seats_left, first, business, economy}\n"

    sql = """
    SELECT * FROM flights
        WHERE src = 'JFK' AND dest = 'DEN' 
        AND travel_date >= CURDATE() AND seats_left > 0
        ORDER BY travel_date
        LIMIT 1;
    """
    cursor.execute(sql)
    write_to_file(q3, "Project1PartA-Query3Output.txt", cursor)

    # Query 4
    q4 = "Query 4: Average occupancy rate (%full) for all flights between two cities\n"
    q4 += "Results from JFK and DEN {src, dest, avg_occupancy_rate} (gets average val of both way flights, makes more sense that way)\n"

    sql = """
    SELECT src, dest, AVG((300 - seats_left) / 300.0 * 100) AS average_occupancy_rate
        FROM flights
        WHERE (src = 'JFK' AND dest = 'DEN') OR (src = 'DEN' AND dest = 'JFK')
        GROUP BY src, dest;
    """
    cursor.execute(sql)
    write_to_file(q4, "Project1PartA-Query4Output.txt", cursor)


# Driver function
def main():
    # Connect to database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="12345678",
        database="flightsDB"
    )
    cursor = db.cursor()

    # TABLES ARE CREATED IN "Project1PartA-SQLQueries.sql" - RUN THAT FILE FIRST 
    #   TO CREATE THE TABLES!

    # Initlize our system, parse the XML and insert the data into the database
    airline = Airline(cursor, db)
    airline.parse_xml()
    airline.insert_flights()
    airline.insert_bookings()


    # Select all flights and store as a csv
    sql = "SELECT * FROM flights"
    cursor.execute(sql)
    write_to_file((",").join(airline.FLIGHT_COLUMNS), "flights.csv", cursor)

    # Select all bookings and store as a csv
    sql = "SELECT * FROM bookings"
    cursor.execute(sql)
    write_to_file("booking_id," + (",").join(airline.RESERVATION_COLUMNS), 
                  "bookings.csv", cursor)
    

    # Perform our example queries and print the results to a file and terminal
    example_queries(cursor)
    print(read_from_file("Project1PartA-Query1Output.txt"))
    print(read_from_file("Project1PartA-Query2Output.txt"))
    print(read_from_file("Project1PartA-Query3Output.txt"))
    print(read_from_file("Project1PartA-Query4Output.txt"))


    # Create our combined CSV
    combined_csv(airline)

    # Finally close the database connection
    db.close()


if __name__ == "__main__":
    main()