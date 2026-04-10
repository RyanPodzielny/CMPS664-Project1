# Project 1 - Part B

from collections import defaultdict
from itertools import combinations
import mysql.connector
from typing import Any
from numpy import dtype
import pandas as pd


class Normalizer:
    def __init__(self):
        # Specific types for makes it simpler to ensure things to work

        # Our database connection and cursor to execute queries on the db
        self.db: Any
        self.cursor: Any

        # Using a dataframe for easy manipulation with 1NF and inserts
        self.csv: pd.DataFrame
        self.relation_name: str
        self.attributes: set[str]
        # Takes form of [({A}, {B}), ...] where A -> B
        self.functional_dependencies: list[tuple[set[str], ...]]
        # Primary keys are expected to be the candidate key for entire relation
        self.primary_keys: set[str]

        # New tables we create is list of dicts: name, attributes, primary_keys
        self.tables: list[dict] = []
        self.table_index: int = 1

        # Modes to run tool on (just BCNF or the entire process)
        self.modes: dict = {
            "bcnf": self.decompose_BCNF, 
            "all": self.decompose_all
        }

    
    # For testing (so we can skip inputs)
    def force_parse(self, csv, relation_name, functional_dependencies, 
                    primary_keys, mode="all"):
        self.csv = csv
        self.display_csv_info()
        
        self.db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="12345678"
        )
        self.cursor = self.db.cursor()

        self.relation_name = relation_name

        self.functional_dependencies = functional_dependencies
        self.combine_functional_dependencies()

        self.primary_keys = primary_keys

        self.decompose(mode)
        self.interactive_query(self.db)


    def parse_sql_connection(self):
        print("--- Relation Information ---")

        # Get SQL connection info from user
        host = input("Enter SQL host: ").strip()
        user = input("Enter SQL username: ").strip()
        password = input("Enter SQL password: ").strip()

        # Try to connect to the database
        try:
            self.db = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
            )
            self.cursor = self.db.cursor()
        except Exception:
            print(f"Error connecting to database")
            return False
        
        print("Successfully connected to MySQL")
        print()

        return True


    def parse_CSV(self):
        # Get the CSV file path
        csv_path = input("Enter the path to the CSV file: ")

        # Try to open
        try:
            self.csv = pd.read_csv(csv_path.strip())
        except Exception:
            print(f"Error reading CSV file")
            return False
        
        # Make the columns our attributes so user doesn't have to input them 
        self.attributes = set(self.csv.columns)
        
        return True


    def parse_relation_name(self):
        # Get relation name
        self.relation_name = input("Enter the relation name: ").strip()

        if self.relation_name == "":
            print("Relation name cannot be empty.")
            return False
        
        return True
    

    def combine_functional_dependencies(self):
        # Helper function to combine functional dependencies with the same left 
        #   hand  side into one dependency with all the right hand sides 
        #   combined. This makes it easier to check for violations and decompose
        #   later on
        combined = defaultdict(set)

        # For all functional dependencies, we want to combine the right hand 
        #   sides if they have the same left hand side
        for left, right in self.functional_dependencies:
            # Using a frozenset as the key since sets aren't hashable but we 
            #   want to treat {A} and {A} as the same key, and not care about 
            #   order
            combined[frozenset(left)] |= right
            
        # Now we can just convert back to the format we had before, but with the
        #  right hand sides combined
        self.functional_dependencies =\
            [(set(left), right) for left, right in combined.items()]


    def parse_functional_dependencies(self):
        self.functional_dependencies = []

        line = input("Enter the functional dependencies (e.g., A->B,C->D): ")

        # Split by commas to get each dependency, cleaning up in process
        unformatted = [dependency.strip() for dependency in line.split(",")]

        # For each dependency, we want to split by arrow to get left and right
        for string in unformatted:
            if "->" not in string:
                print(f"Invalid functional dependency format: {string}")
                return False
            
            # Get which determines which, and clean up any extra spaces
            attribute1, attribute2 = string.split("->")
            attribute1 = attribute1.strip()
            attribute2 = attribute2.strip()

            # If they aren't in our attributes, they cannot be a dependency
            if attribute1 not in self.attributes or \
               attribute2 not in self.attributes:
                print(f"Dependencies must be in the set of attributes.")
                return False

            # Add them as ({left, right})
            self.functional_dependencies.append(({attribute1}, {attribute2}))
        
        # Ensure they are combined if we have same determinant
        self.combine_functional_dependencies()

        return True
    

    def find_candidate_keys(self):
        # Helper function to find all candidate keys for the relation based on
        #   the functional dependencies. This is useful for finding the ideal
        #   primary key, and also for checking if we have a transitive
        #   dependency in 3NF

        candidate_keys = []

        # We want to check every combination of attributes to see if it's a 
        #   candidate key (its closure is the entire set of attributes, and no 
        #   subset of it has that property)
        for index in range(1, len(self.attributes) + 1):
            # Check every combination of attributes of that length to see if it 
            #   can determine the entire set of attributes (is a superkey)
            for combination in combinations(self.attributes, index):
                combination_set = set(combination)
                closure = self.compute_closure(combination_set)

                # If the closure is the entire set of attributes, it's a
                #   superkey
                if closure >= self.attributes:
                    # Check if it's minimal (no subset is a superkey)
                    is_candidate = True
                    
                    # Check every subset of the combination to see if any of 
                    #   them are also superkeys. If they are, then this can't be
                    #   candidate keys since it's not minimal
                    for sub_index in range(1, len(combination) + 1):
                        for subset in combinations(combination, sub_index):
                            subset_set = set(subset)
                            subset_closure = self.compute_closure(subset_set)

                            if subset_closure >= self.attributes:
                                is_candidate = False
                                break

                        if not is_candidate:
                            break
                
                    # We found a candidate key, add it to the list
                    if is_candidate:
                        candidate_keys.append(combination_set)

        return candidate_keys


    def parse_primary_keys(self):
        line = input("Enter the primary key(s): ")

        # Store in set for easy checking
        self.primary_keys = {key.strip() for key in line.split(",")}

        # Not a subset, not valid
        if not self.primary_keys <= self.attributes:
            print("Primary keys must be a subset of the attributes.")
            return False
        
        # Must have at least one
        if len(self.primary_keys) == 0:
            print("No primary keys provided.")
            return False
        
        return True
    

    def display_csv_info(self):
        # Show off what the parsed CSV looks like (just basic info)
        print("\n--- CSV Information ---")
        print(f"Attributes: {', '.join(self.csv.columns)}")
        print(f"Number of rows: {len(self.csv)}")
        print(f"Data types:\n{self.csv.dtypes}")
        print()


    def parse_input(self):
        # Continue until we get proper input
        while True:
            # Want to show off CSV before hand
            if self.parse_CSV():
                self.display_csv_info()
            else:
                continue
            
            # Parse the rest
            if self.parse_sql_connection() and\
               self.parse_relation_name() and\
               self.parse_functional_dependencies() and\
               self.parse_primary_keys():

                break

        print("Input parsed successfully."); print()
    

    def compute_closure(self, attributes):
        # Find every single attribute that can be determined by given set of 
        #   attributes based on the functional dependencies
        while True:
            new_attributes = attributes.copy()

            # For each dependency if the left side is in our set of attributes, 
            #   it means it is functionally determined by our attributes. Thus,
            #   attributes on the right side can also be determined - add it
            for left, right in self.functional_dependencies:
                if left <= new_attributes:
                    new_attributes = new_attributes | right

            # If we didn't add any new attributes it means we found everything 
            #   we can determine, so we can stop
            if new_attributes == attributes:
                break

            # We found something, keep looking for attributes we can determine
            attributes = new_attributes

        return new_attributes
    

    def check_1NF(self):
        print("Checking 1NF: ", end="")

        # Check for anything that appears multi-valued in any of the columns. 
        #   If it appears to be a list, then it's not 1NF. Only way to decompose 
        #   is to split those values into separate rows, creating duplicates

        # Non-exhaustive but good enough
        separators = {",", ";", "|"}

        # Stored as a dict of index to list of (attribute, separator) pairs
        violations = defaultdict(list)

        # Check for any of the separators in attributes - found is multi-valued
        for attribute in self.attributes:
            for index, value in self.csv.iterrows():
                # Not a string means can't have multiple values, so skip it
                if type(value[attribute]) != str:
                    continue

                # If has separators, we want to know where so we can decompose
                for separator in separators:
                    if separator in value[attribute]:
                        violations[index].append((attribute, separator))
        
        # Violations have been found - show where they are
        if len(violations) > 0:
            print("Found 1NF violations!")

            # At each row, we can have multiple columns that fail - show all
            for index, problems in violations.items():
                for attribute, separator in problems:
                    print(f"\t- Row {index}: multi-valued attribute " +\
                          f"'{attribute}'")
        else:
            print("Already in 1NF (no multi-valued attributes).")
            
        return violations


    def check_2NF(self, tables):
        print("Checking 2NF: ", end="")

        # Assumption is that we already checked for 1NF and decomposed

        # Need to check for any partial dependencies: that is any FD where the
        #   left side is a subset of the primary key
        partial_dependencies = []

        # If primary key is single, its already in 2NF
        if len(self.primary_keys) == 1:
            print("Already in 2NF (single primary key).")
            return partial_dependencies

        # Need to check every combination of the primary key, expect full set to
        #   see if anything is determined by part of the key (key > 2 attribute)
        primary_key_subsets = []
        for index in range(1, len(self.primary_keys)):
            for subset in combinations(self.primary_keys, index):
                primary_key_subsets.append(set(subset))
        
        # For every primary key subset, we want to see if it's closure can
        #   determine any attributes that are not in the subset. If it can, then
        #   it means we have a partial dependency 
        for subset in primary_key_subsets:
            # Find what the subset can determine
            closure = self.compute_closure(subset)

            # If it determines anything besides the subset, it violates 2NF
            determines = closure - subset
            if len(determines) > 0:
                partial_dependencies.append((subset, determines, self.tables[0]))     

        # Show any violations found
        if len(partial_dependencies) > 0:
            print("Found 2NF violations!")
            for left, right, table in partial_dependencies:
                print(f"\t- Partial Dependency: {left} -> {right}")
        else:
            print("Already in 2NF (no partial dependencies).")

        return partial_dependencies


    def check_3NF(self):
        print("Checking 3NF: ", end="")

        # We need to check for any transitive dependencies
        transitive = []
        
        # Check on the new tables created from 2NF as we would find the same
        #   dependencies but wouldn't know which table they were in
        for table in self.tables:
            attributes = table["attributes"]
            nonprime = attributes - table["primary_keys"]


            # For all our functional dependencies, if they are any where the 
            #   left side is not a super key, and right side isn't part of the 
            #   candidate key for that table then it is not in 3NF

            # Help from:
            #   https://www.geeksforgeeks.org/dbms/third-normal-form-3nf/
            for left, right in self.functional_dependencies:
                if not left <= attributes or not right <= attributes:
                    continue

                nonprime_rights = right & nonprime

                # A super key is any set of attributes that can determine all 
                #   attributes in the table, i.e. the closure of the set is 
                #   the entire table
                is_superkey = self.compute_closure(left) >= attributes
                is_nonprime = len(nonprime_rights) > 0

                if not is_superkey and is_nonprime:
                    # Store the table for easier decomposition
                    transitive.append((left, nonprime_rights, table))

        # Show any violations found
        if len(transitive) > 0:
            print("Found 3NF violations!")
            for left, right, table in transitive:
                print(f"\t- Transitive Dependency: " +
                      f"{table["primary_keys"]} -> {left} -> {right} " +
                      f"(in table {table["name"]})")
        else:
            print("Already in 3NF (no transitive dependencies).")

        return transitive
    

    def check_BCNF(self):
        print(f"Checking BCNF on {len(self.tables)} table(s): ", end="")

        # We need to check for any functional dependencies where the left hand 
        #   side is not a super key. If we find any, then it is not in BCNF

        violations = []
        for table in self.tables:
            attributes = table["attributes"]

            # For all our functional dependencies, if they are any where the 
            #   left side is not a super key, then it is not in BCNF

            # Help from:
            #   https://www.geeksforgeeks.org/dbms/boyce-codd-normal-form-bcnf/
            for left, right in self.functional_dependencies:
                # Don't care about dependencies that aren't in the table
                if not left <= attributes or not right <= attributes:
                    continue

                # Super key means if left contains all attributes on the table,
                #   i.e. the closure of the left is the entire table
                is_superkey = self.compute_closure(left) >= attributes
                if not is_superkey:
                    violations.append((left, right, table))

                    # We want to break after the first violation as one pass 
                    #   could fragment the table and not catch all violations
                    break
 

        # Show any violations found
        if len(violations) > 0:
            print("Found BCNF violations!")
            for left, right, table in violations:
                print(f"\t- Determinant not a Superkey: {left} -> {right} " +
                      f"on {table["name"]}")
        else:
            print("In BCNF (no dependencies with non-superkey determinant).")

        return violations
    

    def create_table(self, name, attributes, primary_keys, padding="\t\t"):
        # Helper function to create a table and show it
        table = {
            "name": name,
            "attributes": attributes,
            "primary_keys": primary_keys
        }
        print(f"{padding}- Created {table["name"]}: {table["attributes"]}" +
              f" with primary key {table["primary_keys"]}")
        
        self.tables.append(table)
        self.table_index += 1


    def decompose_1NF(self, violations_1NF):
        print(f"\tDecomposing into:")

        # For each violation we want to only expand the max amount of 
        #   values at that specific index. Meaning if we have multiple
        #   multi-valued attributes, we want to expand them together
        for index, problems in violations_1NF.items():
            expansions = {}
            max_copies = 1

            # Get values by their respective separator at each attribute
            for attribute, separator in problems:
                value = self.csv.at[index, attribute]
                expansions[attribute] = str(value).split(separator)
                max_copies = max(max_copies, len(expansions[attribute]))

            # Create max number rows found by the largest expansion. 
            #   If rows don't have same amount, just copy - similar to how we 
            #   copy other attributes when expanding
            for place in range(max_copies):
                new_row = self.csv.iloc[index].copy()

                # Update the values at their respective indices
                for attribute, values in expansions.items():
                    if place < len(values):
                        new_row[attribute] = values[place].strip()
                    else:
                        new_row[attribute] = values[-1].strip()
                
                # Add the new row to the dataframe and show what we did
                self.csv.loc[len(self.csv)] = new_row
                print(f"\t\t- Created new row: {new_row.to_dict()}")
            
            # Remove the original row 
            self.csv.drop(index, inplace=True)
            print(f"\t\t- Removed original row: {index}")

    
    def split_table(self, violations):
        # Helper function to split a table into two based on the left and right
        #   sides of a functional dependency. The left side becomes the primary 
        #   key of the new table, and the right hand side becomes the attributes
        #   of the new table. The old table has the right attributes removed. 
        #   This process is same for 2NF, 3NF, and BCNF

        print(f"\tDecomposing into:")

        for left, right, table in violations:
            self.create_table(
                f"{self.relation_name}Table{self.table_index}",
                left | right,
                left
            )

            table["attributes"] -= right
            print(f"\t\t- {table["name"]}: Removed {right}")


    def decompose_BCNF(self):
        # Check for BCNF violations - need to keep checking until we find no
        #   more in the new tables (decomposing could create other violations)
        while True:
            violations_BCNF = self.check_BCNF()

            if len(violations_BCNF) == 0:
                break
        
            self.split_table(violations_BCNF)


    def decompose_all(self):
        # Check for 1NF violations and decompose if any found
        violations_1NF = self.check_1NF()
        if len(violations_1NF) > 0:
            self.decompose_1NF(violations_1NF)
        
        # Check for 2NF violations 
        violations_2NF = self.check_2NF(self.tables)
        if len(violations_2NF) > 0:
            self.split_table(violations_2NF)

        # Check for 3NF violations
        violations_3NF = self.check_3NF()
        if len(violations_3NF) > 0:
            self.split_table(violations_3NF)

        self.decompose_BCNF()


    def generate_SQL(self):
        # Store queries as {[table_name]: [create, insert, insert, ...]} for
        #   each table
        queries = defaultdict(list)

        for table in self.tables:
            # Get the properties of the table we need
            name = table["name"]
            attributes = table["attributes"]
            primary_keys = table["primary_keys"]

            # Start of with the CREATE TABLE statement 
            query = f"CREATE TABLE {name} (\n"

            # Add all our columns with their types
            for attribute in attributes:
                # We will try to infer the type from the CSV data, but if we 
                #   can't then we will just default to VARCHAR(255)
                if self.csv[attribute].dtype == dtype("int64"):
                    query += f"\t{attribute} INT,\n"
                elif self.csv[attribute].dtype == dtype("float64"):
                    query += f"\t{attribute} FLOAT,\n"
                else:
                    query += f"\t{attribute} VARCHAR(255),\n"

            # Add our primary key constraint, and ending parenthesis
            query += f"\tPRIMARY KEY ({', '.join(primary_keys)})\n);"
            queries[name].append(query)

            # Make all inserts distinct on the primary keys
            table_csv = self.csv.drop_duplicates(list(table["primary_keys"]))

            # After we created the table, go through each row in the CSV and 
            #   make an INSERT for it on their respective new tables
            for _, row in table_csv.iterrows():
                values = []

                for attribute in attributes:
                    value = row[attribute]

                    # Add quotes around string values and terminate any single 
                    #   quotes in the value (so it doesn't break anything)
                    if type(value) == str:
                        value = value.replace("'", "''")
                        value = f"'{value}'"

                    values.append(str(value))

                # Create the SQL query for inserting the row into the table
                query = f"INSERT INTO {name} ({', '.join(attributes)}) " +\
                        f"VALUES ({', '.join(values)});"
                queries[name].append(query)

        # Return to execute
        return queries
    
    
    def execute_SQL(self, queries):
        print("\n\n--- Executing SQL Queries ---")

        # Helper function to execute the SQL queries we generated on the 
        #   database. First we need to create the database, then we can execute
        #   the CREATE TABLE statements and the INSERT statements
        self.cursor.execute(f"DROP DATABASE IF EXISTS {self.relation_name}")
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.relation_name};")
        self.cursor.execute(f"USE {self.relation_name};")

        # Go through each table and execute the queries we generated for it
        for table_name, table_queries in queries.items():
            print(f"Executing table and insert queries for {table_name}...")

            # Try to execute
            for query in table_queries:
                try:
                    self.cursor.execute(query)
                    self.db.commit()
                # Let user know if we couldn't
                except Exception as e:
                    print(f"\t- Error executing query: {e}")
                    continue

            print(f"\t- Successfully inserted {len(table_queries) - 1} rows!")
    

    def print_tables(self):
        # Show off all the tables we created to the user 
        print("\n\n--- Final Tables ---")
        for table in self.tables:
            print(f"{table["name"]}: {table["attributes"]} " +\
                  f"with primary key {table["primary_keys"]}")


    def decompose(self, mode="all"):
        # Get mode selected
        mode = mode.lower().strip()
        if mode not in self.modes:
            print(f"Invalid mode: {mode}. Defaulting to 'all'.")
            mode = "all"

        # Create main table in order to split on
        print("Main Table Based on Input Relation:")
        self.create_table(
            f"{self.relation_name}MainTable",
            self.attributes,
            self.primary_keys,
            padding=""
        )

        print("\n\n--- Normalization Process ---")

        # Run selected mode (already checked for validity)
        self.modes[mode]()

        # Show off final tables to inform user
        self.print_tables()

        # Everything has been decomposed, we can generate our SQL statements
        self.execute_SQL(self.generate_SQL())


    def interactive_query(self, db):
        # Extra feature to allow user to query the tables we created in an 
        #   interactive way. Not required, but could be useful for testing and 
        #   demonstration purposes

        print("\n\n--- Interactive Query Mode ---")
        print("Type 'exit' to quit.")
        print()

        # Continue until user wants to exit
        while True:
            query = input("Enter a SQL query:\n").strip()
            
            # User wants out, let them out
            if query.lower() == "exit":
                print("Exiting interactive query mode.")
                break
            
            # If we can run do it, but if not let user know what went wrong
            try:
                cursor = db.cursor()
                cursor.execute(query)
                results = cursor.fetchall()

                # Print the results in a nice format
                for row in results:
                    print(row)
    
            except Exception as error:
                print(f"Error executing query: {error}")
                continue

    
    def normalize(self, mode="all"):
        print("--- Normalization Tool ---")

        # Parse everything from the user
        self.parse_input()

        # Compute closure to see if functional dependencies are valid and to 
        #   find candidate keys for later use
        print("--- Validating Relation ---")
        closure = self.compute_closure(self.primary_keys)
        if closure != self.attributes:
            print("- Closure of primary keys cannot determine all attributes")

        # If there are candidate keys then we are going to use them as our 
        #   primary keys for the tables we create, so we want to show the user
        #   and inform them we are overriding their input primary keys 
        candidate_keys = self.find_candidate_keys()
        if self.find_candidate_keys() == []:
            print("- No candidate keys found - will need to decompose")
        elif self.find_candidate_keys() != [self.primary_keys]:
            print(f"- Candidate keys found: {candidate_keys}")
            print("- Overriding input primary keys with candidate keys.")
            self.primary_keys = candidate_keys[0]
        else:
            print(f"- Candidate keys match primary keys: {candidate_keys}")

        # Actually normalize and decompose
        self.decompose(mode)

        # Run an interactive query mode for the user to query the tables
        self.interactive_query(self.db)

        # Close the database connection when done
        self.db.commit()
        self.db.close()


# Driver function
def main():
    tool = Normalizer()
    tool.normalize()

    '''
    TESTING 
    tool.force_parse(
        pd.read_csv("combined.csv"),
        relation_name="flight_bookings",
        functional_dependencies=[
            ({"booking_id"}, 
                {"first_name", "last_name", "address", "age", 
                "src", "dest", "travel_date", "class", "booking_time", "npass", 
                "flight_id"}),
            ({"flight_id"}, 
                {"src", "dest", "travel_date", "seats_left", 
                "first", "business", "economy"})
        ],
        primary_keys={"booking_id"},
        mode="all"
    )
    '''
    

if __name__ == "__main__":
    main()  