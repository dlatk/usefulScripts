#!/usr/bin/python

"""
This script imports a csv file to a MySQL table, inferring column types automatically.
Usage:
    csv2mySQL.py FILE DATABASE TABLENAME [IGNORE_LINES]

Adapted by gpt40 from Maarten Sap's csv2mysql.py
"""

import MySQLdb, sys, os, csv
import time, datetime
from warnings import filterwarnings

filterwarnings('ignore', category=MySQLdb.Warning)

DISABLE_KEYS = True

if len(sys.argv) != 5 and len(sys.argv) != 4:
    sys.stderr.write("""Check your arguments!
Usage:       csv2mySQL.py FILE DATABASE TABLENAME [IGNORELINES]
Example:     csv2mySQL.py example.csv my_database my_new_table 1
""")
    sys.exit(1)

start = time.time()
filename = sys.argv[1]
database = sys.argv[2]
table = sys.argv[3]
ignore_lines = int(sys.argv[4]) if len(sys.argv) == 5 else 0
files = [filename]

# Check if input is a directory
if os.path.isdir(filename):
    print("Found a directory, reading ALL files in %s" % filename)
    files = [os.path.abspath(os.path.join(filename, f)) for f in os.listdir(filename) if os.path.isfile(os.path.join(filename, f))]

# Connect to the database
db = MySQLdb.connect(db=database, local_infile=1, read_default_file='~/.my.cnf', use_unicode=True, charset="utf8")
cur = db.cursor()

# Check if table exists
cur.execute("SHOW TABLES LIKE '%s'" % table)
existing_tables = [item[0] for item in cur.fetchall()]
append = False

if len(existing_tables) > 0:
    print("A table by that name already exists in the database... Do you wish to overwrite it? (y/n) (enter 'a' for appending to existing table)")
    answer = sys.stdin.readline()[0]
    if answer.lower() == 'a':
        append = True
    elif answer.lower() != 'y':
        print("Try again with a different table name")
        sys.exit(1)

# Infer column types based on the CSV file
def infer_column_types(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)  # Read headers
        data_types = [None] * len(headers)
        max_lengths = [0] * len(headers)
        row_count = 0

        for row in reader:
            for i, value in enumerate(row):
                if not value:  # Empty value, skip
                    continue

                try:
                    int_val = int(value)
                    if data_types[i] not in ['DOUBLE', 'VARCHAR', 'TEXT', 'DATETIME', 'DATE']:
                        if int_val < 10:
                            data_types[i] = 'TINYINT'
                        else:
                            data_types[i] = 'INT'
                except ValueError:
                    try:
                        float(value)
                        data_types[i] = 'DOUBLE'
                    except ValueError:
                        if is_datetime(value):
                            data_types[i] = 'DATETIME' if ' ' in value else 'DATE'
                        else:
                            max_lengths[i] = max(max_lengths[i], len(value))
                            if data_types[i] not in ['TEXT']:
                                data_types[i] = 'VARCHAR' if max_lengths[i] < 200 else 'TEXT'

            row_count += 1
            if row_count > 1000:  # Only sample up to 1000 rows
                break

    column_definitions = []
    for i, header in enumerate(headers):
        col_type = data_types[i] if data_types[i] else 'VARCHAR(255)'
        if col_type == 'VARCHAR':
            col_type += "(%d)" % max_lengths[i]
        if header.endswith("_id"):
            col_type += " INDEX"
        column_definitions.append(f"`{header}` {col_type}")
    return ", ".join(column_definitions)

# Function to check if a string is a date
def is_datetime(value):
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d'):
        try:
            datetime.datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False

# Create or append to the table
if not append:
    cur.execute("DROP TABLE IF EXISTS %s" % table)
    print("Creating table %s" % table)
    column_description = infer_column_types(files[0])
    create_table_query = f"CREATE TABLE {table} ({column_description}) ENGINE=MyISAM"
    print(create_table_query)
    cur.execute(create_table_query)
else:
    print(f"Appending to table [{table}]")

# Disable keys for faster import
if DISABLE_KEYS:
    print("Altering table to disable keys for faster import")
    cur.execute(f"ALTER TABLE {table} DISABLE KEYS")

# Import data
for i, f in enumerate(files):
    cur.execute(f"""LOAD DATA LOCAL INFILE '{f}' INTO TABLE {table} 
                    FIELDS TERMINATED BY ',' 
                    ENCLOSED BY '"' 
                    LINES TERMINATED BY '\\n' 
                    IGNORE {ignore_lines} LINES""")
    if len(files) != 1 and (i + 1) % 10 == 0:
        print(f"Imported {i + 1} files out of {len(files)}...")

print(f"Imported {len(files)} files out of {len(files)}... Database: {database}, Table: {table}")

# Re-enable keys
if DISABLE_KEYS:
    print("Altering table to enable keys")
    cur.execute(f"ALTER TABLE {table} ENABLE KEYS")

# Commit and close
db.commit()
print("Import completed in %s" % datetime.timedelta(seconds=(time.time() - start)))

