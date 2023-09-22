#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Module:  densifyTable.py
"""

__author__ = 'Lukasz Dziurzynski, Andrew Schwartz'

import os
import sys
import csv
import argparse
sys.path.append("~/dlatk/dlatk")
from dlatk.mysqlMethods import mysqlMethods as mm
from pprint import pprint as pp

def tableToDenseCsv(db, table, row_column, col_column, value_column, output_csv_filename=None, compress_csv=True):
    """Take a mysql table to convert a long table (e.g. feature table) to a dense 2x2 contingency matrix (size N by M where N is the number of distinct rows and M is the number of distinct columns). Efficient (uses lookups instead of a single iteration through all entries of the contingency matrix -- could be more slightly more efficient if it used the dbCursor pointer).
    
    Arguments:
    - `db`: database to pull from
    - `table`: table to pull from
    - `row_column`: `table` column that will populate the rows of the contingency csv
    - `col_column`: `table` column that will populate the columns of the contingency csv
    - `value_column`: `table` column that will populate the values at the intersection of the rows and columns of the contingency csv
    - `output_csv_filename`: the name of the output file -- if empty is created based on the values provided
    - `compress_csv`: whether to gzip the csv
    """

    if not output_csv_filename:
        output_csv_filename = 'dense.{db}.{table}.{row}-by-{col}.{value}.csv'.format(db=db, table=table, row=row_column, col=col_column, value=value_column)

    sorted_row_values = list(mm.qExecuteGetList1(db, 'SELECT DISTINCT {row} FROM {table} ORDER BY {row}'.format(row=row_column, table=table)))
    sorted_col_values = list(mm.qExecuteGetList1(db, 'SELECT DISTINCT {col} FROM {table} ORDER BY {col}'.format(col=col_column, table=table)))

    sorted_values = mm.qExecuteGetList(db, 'SELECT {row}, {col}, {value} FROM {table} ORDER BY {row}, {col}'.format(row=row_column, col=col_column, value=value_column, table=table) )

    N = len(sorted_row_values)
    M = len(sorted_col_values)

    with open(output_csv_filename, 'wb') as output_csv:
        csv_writer = csv.writer(output_csv)
        csv_writer.writerow([row_column] + sorted_col_values)

        current_row = sorted_values[0][0]
        current_column_data = ['NULL'] * M
        num_row_writes = 0

        for tablerow, tablecol, tablevalue in sorted_values:
            # if a new row, write our current column data
            # and reset local variables
            if current_row != tablerow:
                #print "adding %s" % str(current_row)
                csv_writer.writerow([current_row] + current_column_data)
                current_column_data = ['NULL'] * M
                num_row_writes += 1
                current_row = tablerow
                if num_row_writes % 1000 == 0:
                    print '{n} out of {N} rows complete'.format(n=num_row_writes, N=N)
            column_index = sorted_col_values.index(tablecol)
            current_column_data[column_index] = tablevalue

        csv_writer.writerow([current_row] + current_column_data)
        #print "adding %s" % str(current_row)
        print "wrote %d features over %d rows" % (N, num_row_writes)


    if compress_csv:
        # os.system("gzip {output_filename}".format(output_filename=output_csv_filename)
        pass

def main():
    """Main Docstring"""
    parser = argparse.ArgumentParser(description='Create a dense csv given a db, table, and three columns.')
    parser.add_argument('-d', '--db', dest='db', type=str, default='',
                       help='database')
    parser.add_argument('-t', '--table', dest='table', type=str,
                       help='the table to make dense')
    parser.add_argument('-r', '--row', dest='row', type=str,
                       help='the row to use from table')
    parser.add_argument('-c', '--col', dest='col', type=str,
                       help='the col to use from table')
    parser.add_argument('-v', '--value', dest='value', type=str,
                       help='the value to use from table')
    parser.add_argument('-f', '--csv_filename', dest='csvfilename', type=str, default='',
                       help='optional, the output filename.')
    args = parser.parse_args()

    if not args.db or not args.table or not args.row or not args.col or not args.value:
        parser.error('Must specify db, table, row, col, and value to run.')

    tableToDenseCsv(args.db, args.table, args.row, args.col, args.value, args.csvfilename)

    # this is an example use case
    # tableToDenseCsv('twitterGH', 'z_featureExport', 'group_id', 'feat', 'group_norm', 'test.csv')

    return 0

if __name__=='__main__':
    sys.exit(main())

