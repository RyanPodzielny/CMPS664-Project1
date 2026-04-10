# Project 1 - Part A

## Overview

`Project1PartA-Code.py` is a airline system that takes in a PNR.xml file to 
create a flight reservation system based on the bookings presented in the data. 
It parses the xml data, determines flights needed to accommodate the bookings
and generates three CSV files and as well a MySQL database. It's also serves as
a example ususage for the normalization tool in Part B. 

## Requirements

- Python 3
- A running MySQL server
- The PNR.xml file

## How To Run

From the project folder, run:

```
python Project1PartA-Code.py
```

This is non-interactive and performs as a script.

## Output

The script will generate three CSV files, one for the bookings. Containing all
passenger information and the flight the passenger will be on. One for the
flights data, which stores all the created flights based on the bookings. And
another for the combined of the two - this is purely to showcase the 
normalization tool in Part B as it is a suboptimal design for the database. The
script also showcases the relation with the four example queries listed in the
project description. 

`Project1PartA-SQLQuries.sql` contains the database and table creation needed
to insert the data generated for the script and needs to be ran before it. It
also contains the example queries to showcase the reservation system.


# Project 1 - Part B

## Overview

`Project1PartB-Code.py` is an interactive normalization tool for a CSV relation.
It reads a CSV file, connects to MySQL, checks the relation for 1NF, 2NF, 3NF,
and BCNF issues, decomposes the table as needed, generates SQL, and then opens
an interactive query prompt against the created database.

## Requirements

- Python 3
- A running MySQL server

## How To Run

From the project folder, run:

```
python Project1PartB-Code.py
```

The tool is interactive and will prompt you for the following information in
order:

1. CSV file path
2. MySQL host
3. MySQL username
4. MySQL password
5. Relation name
6. Functional dependencies
7. Primary key(s)


## Input Format

### CSV File

The CSV header row is used as the set of attributes for the relation. Each CSV
column becomes an attribute that can be used in functional dependencies and
primary keys.

### Functional Dependencies

Enter dependencies as a comma-separated list using the form 
`determinate->dependent`

Example:

```text
booking_id->first_name, booking_id->last_name, flight_id->src, flight_id->dest
```

Notes:

- Each attribute mentioned in the dependencies must exist in the CSV header
- The tool combines dependencies with the same left-hand side automatically
- Only one attribute can exist on either side of the `->` symbol


### Primary Keys

Enter one or more primary key attributes separated by commas.

Example:

```text
booking_id
```

or

```text
booking_id, flight_id
```

## What The Tool Does

After the inputs are accepted, the script:

1. Validates the relation using the provided primary key(s)
2. Searches for candidate keys and computes closure to validate the relation
3. Creates a main table for the input relation
4. Decomposes the relation based on normalization violations
5. Creates a MySQL database named after the relation
6. Generates and executes `CREATE TABLE` and `INSERT` statements for each 
    decomposed table
7. Opens an interactive SQL prompt to allow for queries on newly formed database

## Interactive Query Mode

After the tables are created, you can type SQL queries directly into the prompt.
Type `exit` to leave the query loop.

Example:

```sql
SELECT * FROM airlineMainTable;
```

## Important Notes for the Tool

- The tool will drop and recreate the MySQL database with the same name as the
	relation you enter. This is to prevent multiple inserts and faulty database
    creation. It is suggested to use a database name that is not in use
- The default normalization mode in the current script is 'all' which performs
    both 1NF, 2NF, 3NF and BCNF in succesion. Optionally users can specify to
    only use BCNF avoid the checking of 1NF which takes a decent amount of time
- Multi-valued attributes are detected using common separators such as comma,
	semicolon, and pipe
- If the CSV contains string values with apostrophes, the generated SQL escapes
	them automatically

## Typical Usage

1. Prepare a CSV file with a header row
2. Start MySQL and make sure you know the host, username, and password
3. Run `python Project1-PartB.py`
4. Enter the CSV path and relation metadata when prompted
5. Review the generated tables and SQL output
6. Run SQL queries in the interactive prompt or type `exit` to quit