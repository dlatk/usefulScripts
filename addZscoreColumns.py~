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
    print >>sys.stderr,"SQL:\t"+query[:250]
    cur.execute(query)
    return cur

def getCols(cur, tables, cols, group_bys = None, where = None):
    query = ""
    if len(tables) == 1:
        query = "select %s, %s from %s where %s is not null and %s is not null" % (cols[0], cols[1], tables[0], cols[0], cols[1])
    elif group_bys:
        query = """select a.%s, b.%s
from %s as a, %s as b
where a.%s is not null and b.%s is not null and a.%s = b.%s""" % (cols[0], cols[1], tables[0], tables[1], cols[0], cols[1], group_bys[0], group_bys[-1])
    if where:
        query += " and " + where
    try:
        cur = _execute(cur, query)
    except Exception as e:
        if e[0] == 1052:
            print >>sys.stderr,"\nERROR: make the where clause less ambiguous by adding an 'a.' or 'b.' depending on which table the column you're restricting is in.\n"
        else:
            print >>sys.stderr,e
        exit()
    both = [[j for j in i ] for i in cur]

    x = np.array([i[0] for i in both])
    y = np.array([i[1] for i in both])
    return x, y

def main(args):
    print >>sys.stderr, "Correlate two columns in MySQL tables (-h for help)"
    cur = _connect(args.db)
    if len(args.tables) == 1:
        args.tables = args.tables + args.tables

    x, y = getCols(cur, args.tables, args.cols, args.group_bys, args.where)
    N = len(x)
    
    r, p = pearsonr(x,y)
    
    if args.csv:
        print ', '.join(args.cols)+", %f, %f, %d" % (r, p, N)
    else:
        print "\nOutcomes:        "+', '.join(args.cols) + "%s" % (" [where %s]" % str(args.where) if args.where else "")
        print "Pearson r:      %9.6f" % r
        print "p-value:        %9.6f" % p
        print "N:               %d" % N

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Correlate two columns in a MySQL table.')
    parser.add_argument('-d','--database', dest='db',
                        help='name of the database that contains the table')
    parser.add_argument('-t', '--table', '--tables', dest='tables', nargs="+",
                        help='name of the table')
    parser.add_argument('-c', '--columns', dest='cols', nargs="+",
                        help='columns to correlate')
    parser.add_argument('-g', '--group_bys', dest="group_bys", nargs="*",
                        help='If using two tables, what columns to group by')
    parser.add_argument('-w', '--where', dest='where', default=None,
                        help='where field for the MySQL query')
    parser.add_argument('--csv', dest='csv', action='store_true',
                        help='also output to csv file')

    args = parser.parse_args()
    if len(sys.argv) == 1 or len(args.cols) != 2:
        parser.print_help()
    else:
        print >>sys.stderr, args
        main(args)
