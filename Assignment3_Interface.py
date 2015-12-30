#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import thread
import time

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'movieid'
SORT_COLUMN_NAME_SECOND_TABLE = 'movieid'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'movieid'
RANGE_PART_PREFIX = 'range_part'
RANGE_PART_PREFIX2 = 'table2_range_part'
##########################################################################################################

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
    cur.execute("CREATE TABLE "+ratingstablename+" (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")

    loadout = open(ratingsfilepath,'r')

    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")

#    cur.close()
    openconnection.commit()


def rangepartition(ratingstablename, SortingColumnName, numberofpartitions, temporaryTable, newColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS {0};".format(temporaryTable))
    cur.execute("CREATE TABLE {0}(LIKE {1});".format(temporaryTable, ratingstablename))
    cur.execute("INSERT INTO {0} SELECT * FROM {1};".format(temporaryTable, ratingstablename))
    cur.execute("SELECT MIN({0}), MAX({1}) FROM {2};".format(SortingColumnName, SortingColumnName, temporaryTable))
    min_max = cur.fetchall()[0]
    min_value = min_max[0]
    max_value = min_max[1]
    range_values = max_value - min_value

    window = range_values/numberofpartitions
    incr = 0
    partition = 1
    counter = 1
    while partition < numberofpartitions+1:
        cur.execute("DROP TABLE IF EXISTS  {0}{1};".format(RANGE_PART_PREFIX, partition))
        cur.execute("CREATE TABLE {0}{1}(LIKE {2});".format(RANGE_PART_PREFIX, partition, temporaryTable))
        cur.execute("ALTER TABLE {0}{1} ADD COLUMN {2} BIGSERIAL;".format(RANGE_PART_PREFIX, partition, newColumn))
        openwindow = incr*window
        closewindow = (incr+1)*window
        if partition == numberofpartitions:
            cur.execute("INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {5} <= {6};".format(RANGE_PART_PREFIX, partition, temporaryTable, SortingColumnName, openwindow, SortingColumnName, max_value))
            cur.execute("SELECT COUNT(*) FROM {0}{1};".format(RANGE_PART_PREFIX, partition))
            total = cur.fetchall()[0][0]
            counter = counter + total
        else:
            cur.execute("INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {5} < {6};".format(RANGE_PART_PREFIX, partition, temporaryTable, SortingColumnName, openwindow, SortingColumnName, closewindow))
            cur.execute("SELECT COUNT(*) FROM {0}{1};".format(RANGE_PART_PREFIX, partition))
            total = cur.fetchall()[0][0]
            counter += total
        incr = incr + 1
        partition +=1
    openconnection.commit()
    cur.close()

def rangepartition2(ratingstablename, SortingColumnName, numberofpartitions, temporaryTable, newColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS {0};".format(temporaryTable))
    cur.execute("CREATE TABLE {0}(LIKE {1});".format(temporaryTable, ratingstablename))
    cur.execute("INSERT INTO {0} SELECT * FROM {1};".format(temporaryTable, ratingstablename))
    cur.execute("SELECT MIN({0}), MAX({1}) FROM {2};".format(SortingColumnName, SortingColumnName, temporaryTable))
    min_max = cur.fetchall()[0]
    min_value = min_max[0]
    max_value = min_max[1]
    range_values = max_value - min_value

    window = range_values/numberofpartitions
    incr = 0
    partition = 1
    counter = 1
    while partition < numberofpartitions+1:
        cur.execute("DROP TABLE IF EXISTS  {0}{1};".format(RANGE_PART_PREFIX2, partition))
        cur.execute("CREATE TABLE {0}{1}(LIKE {2});".format(RANGE_PART_PREFIX2, partition, temporaryTable))
        openwindow = incr*window
        closewindow = (incr+1)*window
        if partition == numberofpartitions:
            cur.execute("INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {5} <= {6};".format(RANGE_PART_PREFIX2, partition, temporaryTable, SortingColumnName, openwindow, SortingColumnName, max_value))
            cur.execute("SELECT COUNT(*) FROM {0}{1};".format(RANGE_PART_PREFIX2, partition))
            total = cur.fetchall()[0][0]
            counter = counter + total
        else:
            cur.execute("INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {5} < {6};".format(RANGE_PART_PREFIX2, partition, temporaryTable, SortingColumnName, openwindow, SortingColumnName, closewindow))
            cur.execute("SELECT COUNT(*) FROM {0}{1};".format(RANGE_PART_PREFIX2, partition))
            total = cur.fetchall()[0][0]
            counter += total
        incr = incr + 1
        partition +=1
    openconnection.commit()
    cur.close()


def rangepartition3(ratingstablename, SortingColumnName, numberofpartitions, temporaryTable, newColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS {0};".format(temporaryTable))
    cur.execute("CREATE TABLE {0}(LIKE {1});".format(temporaryTable, ratingstablename))
    cur.execute("INSERT INTO {0} SELECT * FROM {1};".format(temporaryTable, ratingstablename))
    cur.execute("SELECT MIN({0}), MAX({1}) FROM {2};".format(SortingColumnName, SortingColumnName, temporaryTable))
    min_max = cur.fetchall()[0]
    min_value = min_max[0]
    max_value = min_max[1]
    range_values = max_value - min_value

    window = range_values/numberofpartitions
    incr = 0
    partition = 1
    counter = 1
    while partition < numberofpartitions+1:
        cur.execute("DROP TABLE IF EXISTS  {0}{1};".format(RANGE_PART_PREFIX, partition))
        cur.execute("CREATE TABLE {0}{1}(LIKE {2});".format(RANGE_PART_PREFIX, partition, temporaryTable))
        openwindow = incr*window
        closewindow = (incr+1)*window
        if partition == numberofpartitions:
            cur.execute("INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {5} <= {6};".format(RANGE_PART_PREFIX, partition, temporaryTable, SortingColumnName, openwindow, SortingColumnName, max_value))
            cur.execute("SELECT COUNT(*) FROM {0}{1};".format(RANGE_PART_PREFIX, partition))
            total = cur.fetchall()[0][0]
            counter = counter + total
        else:
            cur.execute("INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {5} < {6};".format(RANGE_PART_PREFIX, partition, temporaryTable, SortingColumnName, openwindow, SortingColumnName, closewindow))
            cur.execute("SELECT COUNT(*) FROM {0}{1};".format(RANGE_PART_PREFIX, partition))
            total = cur.fetchall()[0][0]
            counter += total
        incr = incr + 1
        partition +=1
    openconnection.commit()
    cur.close()


def create_table(OutputTable, temporaryTable, newColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + OutputTable)
    cur.execute("CREATE TABLE {0}(LIKE {1});".format(OutputTable, temporaryTable))
    cur.execute("ALTER TABLE {0} ADD COLUMN {1} NUMERIC ;".format(OutputTable, newColumn))


def sort(inputTable, SortingColumnName, OutputTable, newColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("INSERT INTO {0} SELECT * FROM {1} ORDER BY {2}, {3};".format(OutputTable, inputTable, SortingColumnName, newColumn))


def column_names(InputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute('SELECT column_name FROM information_schema.columns WHERE table_name=%s;', (InputTable, ))
    return [i[0] for i in cur.fetchall()]


def create_join_table(OutputTable, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, newColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + OutputTable)

    columns_table1 = column_names(InputTable1, openconnection)
    columns1 = []
    for col in columns_table1:
        columns1.append('table1.' + str(col) + ' AS table1' + str(col))

    columns_table2 = column_names(InputTable2, openconnection)
    columns2 = []
    for col in columns_table2:
        columns2.append('table2.' + str(col) + ' AS table2' + str(col))
    join_criteria = "SELECT {0}, {1} FROM {2} table1, {3} table2 WHERE table1.{4} = table2.{5} LIMIT 0"\
        .format(','.join(columns1), ','.join(columns2), InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn)
    cur.execute("CREATE TABLE {0} AS ({1});".format(OutputTable, join_criteria))
#    cur.execute("ALTER TABLE {0} ADD COLUMN {1} NUMERIC ;".format(OutputTable, newColumn))
    cur.close()
    openconnection.commit()


def join_tables(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    join_criteria = "SELECT * FROM {0} table1, {1} table2 WHERE table1.{2} = table2.{3}"\
        .format(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn)
    cur.execute("INSERT INTO {0} {1};".format(OutputTable, join_criteria))


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    number_of_threads = 5
    temporaryTable = 'temporaryTable'
    newColumn = 'SEQUENCE'
    rangepartition(InputTable, SortingColumnName, number_of_threads, temporaryTable, newColumn, openconnection)
    create_table(OutputTable, temporaryTable, newColumn, openconnection)

    for i in range(1, number_of_threads + 1):
        thread.start_new_thread(sort, (RANGE_PART_PREFIX + str(i), SortingColumnName, OutputTable, newColumn, openconnection))
    time.sleep(20)
    cur = openconnection.cursor()
    cur.execute("ALTER TABLE {0} DROP COLUMN {1};".format(OutputTable, newColumn))
    cur.close();
    openconnection.commit()


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    number_of_threads = 5
    temporaryTable = 'temporaryTable'
    temporaryTable2 = 'temporaryTable2'
    newColumn = 'SEQUENCE'
    rangepartition3(InputTable1, Table1JoinColumn, number_of_threads, temporaryTable, newColumn, openconnection)
    rangepartition2(InputTable2, Table2JoinColumn, number_of_threads, temporaryTable2, newColumn, openconnection)
    create_join_table(OutputTable, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, newColumn, openconnection)

    for i in range(1, number_of_threads + 1):
        thread.start_new_thread(join_tables, (RANGE_PART_PREFIX + str(i), InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    time.sleep(20)
    openconnection.commit()




################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
