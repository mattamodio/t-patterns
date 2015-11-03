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

NT_list = ['ts', 'cat', 'claimnumber', 'claim_note_id', 'notetypes', 'create_row_ts']

PC_list = ['ts', 'cat', 'opendate', 'closedate', 'state', 'claimnumber', 'callid', 'calltype', \
'calldirection', 'initiateddate', 'initiateddatetimegmt', 'connecteddate', 'connecteddatetimegmt', \
'terminateddate', 'terminateddatetimegmt', 'calldurationseconds', 'holddurationseconds', 'linedurationseconds']

AL_list = ['ts', 'cat', 'claimnumber', 'itemtype', 'activitycd', 'activitydesc', 'activity', 'activitydate', 'paymentamount']

ROWS = 170160
#ROWS = 1000



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

def main(args):

	# get things ready
	print "Starting at {0}...\n".format(datetime.datetime.now())
	
	dbCursor = establishDBconnection(DB)

	tpattern = TPattern.TPattern()

	tpattern.setDB(DB, TABLE, dbCursor, EVENT_TYPE_COLUMN)


	tpattern.timeToNextEvent('Claim Activity Log')


	print "\nFinished at {0}!".format(datetime.datetime.now())


if __name__ == "__main__":
	main(sys.argv)