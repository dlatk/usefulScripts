#!/bin/bash

# Use as following: 
# ./mysqlToCSV.bash fb20 "select * from messages_en" > myDump.csv
# 'fb20' is database; 'messages_en' is table; 'myDump.csv' is CSV dump file

if [ $# -lt 2 ];then
    echo "Use as following:"
    echo './mysqlToCSV.bash fb20 "select * from messages_en" [mysql_options ... ] > myDump.csv'
fi

database="$1"
command_string="$2"
if [ $# -eq 3 ]; then
    mysql "$database" -B -e "$command_string" "$3"| sed 's/\r//g;s/"/""/g;s/^/"/;s/$/"/;s/\t/","/g;s/"NULL"/""/g'
else
    mysql "$database" -B -e "$command_string"|      sed 's/\r//g;s/"/""/g;s/^/"/;s/$/"/;s/\t/","/g;s/"NULL"/""/g'
fi

