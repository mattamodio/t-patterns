import sys
import shelve
import datetime
#import MySQLdb
import matplotlib.pyplot as plt
import TPattern

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

def testEventsIterator(tpattern):
	with open('fakedata.csv', 'r') as f:
		for line in f:
			events = line.strip().split(',')
			events = map(lambda x: (datetime.datetime.strptime(x.split('-')[0], "%Y/%m/%d %H:%M:%S"), x.split('-')[1]), events)
			events = map(lambda x: TPattern.EventInstance(TPattern.EventType(x[1], x[1]), x[0], x[0]), events)
			observationPeriod = TPattern.ObservationPeriod(events)
			yield observationPeriod

def main(args):

	# get things ready
	print "Starting at {0}...\n".format(datetime.datetime.now())
	
	#cledbCursor = establishDBconnection(DB)
	tpattern = TPattern.TPattern()
	#tpattern.setDB(DB, TABLE, dbCursor, EVENT_TYPE_COLUMN)

	anotherLevelExists = True
	while anotherLevelExists:
		print "\nStarting loop, tpatterns found: {0}".format(tpattern.t_patterns_found)


		anotherLevelExists = False

		for observationPeriod in testEventsIterator(tpattern):
			observationPeriod = tpattern.addTpatternsToObservationPeriod(observationPeriod)
			tpattern.processObservationPeriod(observationPeriod)

		#print "Event distributions: {0}".format(tpattern.eventDistributions.keys())
		#print "Event counts: {0}\n".format(tpattern.eventTypeCounts)
		anotherLevelExists = tpattern.processDistributions()
		
	
	tpattern.completenessCompetition()


	print "\nFinished at {0}!".format(datetime.datetime.now())


if __name__ == "__main__":
	main(sys.argv)