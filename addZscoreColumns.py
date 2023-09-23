#!/usr/bin/env python

import sys, argparse
import getpass, MySQLdb
import numpy as np
from scipy.stats.stats import pearsonr

def _connect(db):
    user = getpass.getuser()
    conn = MySQLdb.connect(read_default_file="~/.my.cnf", db=db, user=user)
    return conn.cursor()

def _execute(cur, query):
    query = query.replace('\n', '\n\t')
    print("SQL:\t"+query[:250], file=sys.stderr)
    cur.execute(query)
    return cur.fetchall()


def getAvgStdForCol(cur, table, col, where = None):
    print(col, table)
    query = "SELECT avg(%s), std(%s) FROM %s" % (col, col, table)
    if where:
        query += " WHERE " + where
    result = _execute(cur, query)
    return result[0][0], result[0][1]


def addZScoreForCol(cur, table, col, mean, std, where = None):
    #1. add column
    query = "ALTER TABLE %s ADD %s_z double AFTER %s" % (table, col, col)
    _execute(cur, query)

    #2. update to zscore
    query = "update %s SET %s_z = (%s - %s)/%s" % (table, col, col, mean, std)
    if where:
        query += " WHERE " + where
    _execute(cur, query)



def main(args):
    print("Add a zscored version of a column in a MySQL table (-h for help)")
    cur = _connect(args.db)

    for col in args.cols:
        print("ZSCORING %s in %s" % (col, args.table))
        mean, std = getAvgStdForCol(cur, args.table, col, args.where)
        addZScoreForCol(cur, args.table, col, mean, std, args.where)

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add a zscored column.')
    parser.add_argument('-d','--database', dest='db',
                        help='name of the database that contains the table')
    parser.add_argument('-t', '--table', dest='table',
                        help='name of the table')
    parser.add_argument('-c', '--columns', dest='cols', nargs="+",
                        help='columns to zscore')
    parser.add_argument('-w', '--where', dest='where', default=None,
                        help='where field for the MySQL query')

    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_help()
    else:
        print(args)
        main(args)
