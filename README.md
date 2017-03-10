# Useful Scripts

These scripts 


### mysqlToCSV.bash

Export MySQL data into a CSV

Usage:

```sh
./mysqlToCSV.bash database "sql select statement" [mysql_options ... ] > csv_file
```

Example: 

```sh
./mysqlToCSV.bash fb20 "select * from messages_en" [mysql_options ... ] > myDump.csv
```


### mysqlToJSON.py

Export MySQL data into JSON.

Usage:

```sh
mysqlToJSON.py db 'command' [dict | list] > json_file
```

Example (to JSON): 

```sh
./mysqlToJSON.py twitterGH 'select message_id, message, created_time from messages_en limit 2' 
> {"created_time": "2009-06-12 02:38:26", "message": "@myfriend look at my fancy tweet", "message_id": "99999999"}
> {"created_time": "2009-06-12 02:38:26", "message": "love the tweet!", "message_id": "888888888"}
```

Example (to list): 

```sh
./mysqlToJSON.py twitterGH 'select message_id, message, created_time from messages_en limit 2' list
> ["99999999", "@myfriend look at my fancy tweet", "2009-06-12 02:38:26"]
> ["888888888", "love the tweet!", "2009-06-12 02:38:26"]
```

### csv2mySQL.py

Upload a CSV to MySQL.

Usage:

```sh
csv2mySQL.py FILE DATABASE TABLENAME '(mysql column description)' [IGNORELINES]
```

Example: 

```sh
python csv2mySQL.py example.csv my_database my_new_table '(id int(10), name varchar(20))' 1
```

### tsv2mySQL.py

Upload a TSV to MySQL/

Usage:

```sh
python tsv2mySQL.py FILE DATABASE TABLENAME '(mysql column description)' [IGNORELINES]
```

Example: 

```sh
python tsv2mySQL.py example.tsv my_database my_new_table '(id int(10), name varchar(20))' 1
```

## License

Licensed under a [GNU General Public License v3 (GPLv3)](https://www.gnu.org/licenses/gpl-3.0.en.html)

## Background

Developed by the [World Well-Being Project](http://www.wwbp.org) based out of the University of Pennsylvania.