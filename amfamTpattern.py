import sys
import shelve
import datetime
import MySQLdb
import matplotlib.pyplot as plt
import TPattern

SHELVE_FILE = '/Users/matthewamodio/Documents/School Code/research/AmFam/TimeLineFeatures/Events/claims170k_03112015_new.shelve'
DB = 'amfam'
#TABLE = 'claim_events'
TABLE = 'copy'
EVENT_TYPE_COLUMN = 'eventtype'

ROWS = 170160



def establishDBconnection(dbName):
    '''
    Establishes connection to DB, turns on autocommit, and gets all of the column names from the table and puts them
    into the FIELDS_FOUND variable.
    '''

    # establish connection to DB
    conn = MySQLdb.connect( host='127.0.0.1', user='root', db=dbName, charset='utf8')
    conn.autocommit(True)

    cursor = conn.cursor()

    return cursor

def formSQLinsert(dictToInsert, tableToInsertInto):
    columns = '('
    values = '('
    for k,v in dictToInsert.iteritems():
        columns += "`" + k + "`" + ","
        values += "'" + cleanseValue(v) + "'" + ","
    columns = columns[:-1] + ')'
    values = values[:-1] + ')'

    return "INSERT IGNORE INTO {0} {1} \
            VALUES {2} \
            ".format(tableToInsertInto, columns, values)

def addToTable(data, table, dbCursor):
    '''
    Adds the data in dictionary form to the table, provided the keys are already columns in the table.
    '''

    dbCursor.execute(  formSQLinsert(data, table)  )

def cleanseValue(cellString):
    cellString = cellString.replace("'", "''")
    return cellString

def shelveToSQL():
	d = shelve.open(SHELVE_FILE)

	#################################################################
	# go through each row, printing updates every 5k out of the 170k
	for i in xrange(1,ROWS):


		if i % 5000 == 0:
			print "Row {0} at {1}".format(i, datetime.datetime.now())
		
		row = d[str(i)]

		for event in row:
			if event[1] == 'PC': colList = PC_list
			elif event[1] == 'AL': colList = AL_list
			elif event[1] == 'NT': colList = NT_list

			data = {}
			def assign(zipped):
				col, value = zipped
				data[col] = value
			map(assign, zip(colList, event))

			addToTable(data, TABLE, dbCursor)
	#################################################################

	# clean up
	d.close()

def eventsIterator(tpattern):

	# find out how many observations there are
	tpattern.dbCursor.execute( "SELECT MAX(claimnumber) FROM copy")
	numberOfObservationPeriods = tpattern.dbCursor.fetchall()[0][0]

	# query one observation period at a time
	observationPeriod = 1
	while observationPeriod <= numberOfObservationPeriods:
		q = "SELECT ts,eventtype FROM copy WHERE claimnumber={0}".format(observationPeriod)
		tpattern.dbCursor.execute(q)
		events = tpattern.dbCursor.fetchall()
		observationPeriod +=1
		yield events

def testEventsIterator():
	with open('fakedata.csv', 'r') as f:
		for line in f:
			events = line.strip().split(',')
			events = map(lambda x: (x.split('-')[0], x.split('-')[1]), events)
			events = map(lambda x: (datetime.datetime.strptime(x[0], "%Y/%m/%d %H:%M:%S"), x[1]), events)
			yield events

def main(args):

	# get things ready
	print "Starting at {0}...\n".format(datetime.datetime.now())
	
	#dbCursor = establishDBconnection(DB)
	tpattern = TPattern.TPattern()
	#tpattern.setDB(DB, TABLE, dbCursor, EVENT_TYPE_COLUMN)

	for events in testEventsIterator():
		tpattern.buildDistributions(events)
	
	#for events in eventsIterator(tpattern):
	#	tpattern.buildDistributions(events)
		
		

	tpattern.processDistributions()



	print "\nFinished at {0}!".format(datetime.datetime.now())


if __name__ == "__main__":
	main(sys.argv)