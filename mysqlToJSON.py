#!/usr/bin/env python

import json
import MySQLdb
import sys

__doc__="""mysqlToJSON.py db 'command' [dict | list] > dumpFile.json
Examples:
   1) ./mysqlToJSON.py twitterGH 'select message_id, message, created_time from messages_en limit 2' 
      or ./mysqlToJSON.py twitterGH 'select message_id, message, created_time from messages_en limit 2' dict
      will print:
      {"created_time": "2009-06-12 02:38:26", "message": "@EaseDaMan the most fun/chaotic day, im bringing blades and condoms http://ff.im/3Si1H", "message_id": "2125869204"}
      {"created_time": "2009-06-12 02:38:26", "message": "@lucasnobre  A record est", "message_id": "2125869202"}
   2) ./mysqlToJSON.py twitterGH 'select message_id, message, created_time from messages_en limit 2' list
      will print:
      ["2125869204", "@EaseDaMan the most fun/chaotic day, im bringing blades and condoms http://ff.im/3Si1H", "2009-06-12 02:38:26"]
      ["2125869202", "@lucasnobre  A record est", "2009-06-12 02:38:26"]

"""

if not (len(sys.argv) == 3 or (len(sys.argv) == 4 and sys.argv[3] in ["list", "dict"])):
    print >> sys.stderr, "\nCheck your arguments"
    print >> sys.stderr, __doc__
    sys.exit(1)

db = sys.argv[1]
command = sys.argv[2]
nameColumns = (sys.argv[3] == "dict") if len(sys.argv) == 4 else True

conn = MySQLdb.connect(db=db,read_default_file="~/.my.cnf")
cur = conn.cursor()

cur.execute(command)

def stringify(row_item):
    if isinstance(row_item,unicode) or isinstance(row_item,str):
        return unicode(row_item,'utf-8','ignore')
    return str(row_item)

if nameColumns:
    columnNames = [i[0] for i in cur.description]
    for row in cur:
        print json.dumps({columnNames[i]: stringify(row_item) for i, row_item in enumerate(row)})
else:
    for row in cur:
        print json.dumps([stringify(item) for item in row])
