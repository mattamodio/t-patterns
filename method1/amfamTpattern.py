import sys
import shelve
import datetime
#import MySQLdb
import matplotlib.pyplot as plt
import TPattern
import ast
import bisect
import csv

DB = 'amfam'
#TABLE = 'claim_events'
TABLE = 'copy'
EVENT_TYPE_COLUMN = 'eventtype'
ROWS = 170160

with open('limit_event_types.txt') as f:
	LIMIT_EVENT_TYPES = ast.literal_eval(f.read())

CSV_FILE = '/Users/matthewamodio/Desktop/claim_events.csv'
CSV_FILE = '/Users/matthewamodio/Desktop/claim_events_personrows.csv'

def wholeCsvIterator(limit=None, limitEventTypes=False, printevery=None):
	with open(CSV_FILE, 'r') as f:
		counter=1

		for person in f:
			person = ast.literal_eval(person)
			
			if printevery and counter % printevery==0:
				print counter
				sys.stdout.flush()
			counter+=1

			if limit and counter>limit:
				break

			events = []

			for instance in person:
				event_id, claimnumber, timestamp, event_type = instance
				timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
				event = TPattern.EventInstance(TPattern.EventType(name=event_type), event_id, timestamp, timestamp)
				events.append(event)

			if limitEventTypes:
				events = filter(lambda x: x.event_type.name in LIMIT_EVENT_TYPES, events)
				if not events:
					continue

			observationPeriod = TPattern.ObservationPeriod(events)
			yield observationPeriod


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
	tpattern.dbCursor.execute( "SELECT MAX(claimnumber) FROM {0}".format(TABLE))
	numberOfObservationPeriods = tpattern.dbCursor.fetchall()[0][0]
	numberOfObservationPeriods = 100

	# query one observation period at a time
	row_count = 0
	while row_count <= numberOfObservationPeriods:
		row_count +=1
		q = "SELECT id,ts,eventtype FROM {0} WHERE claimnumber={1} AND eventtype in {2}".format(TABLE, row_count, LIMIT_EVENT_TYPES)
		tpattern.dbCursor.execute(q)
		events = tpattern.dbCursor.fetchall()
		if not events:
			continue
		events = map(lambda x: TPattern.EventInstance(TPattern.EventType(name=x[2]), x[0], x[1], x[1]), events)
		observationPeriod = TPattern.ObservationPeriod(events)
		yield observationPeriod

def testEventsIterator(tpattern):
	with open('fakedata.csv', 'r') as f:
		eventID = 1
		for line in f:
			observationPeriod = TPattern.ObservationPeriod()
			events = line.strip().split(',')
			for event in events:
				time, event_type = event.split('-')
				time = datetime.datetime.strptime(time, "%Y/%m/%d %H:%M:%S")
				event = TPattern.EventInstance(TPattern.EventType(name=event_type), eventID, time, time)
				observationPeriod.addEvent(event)
				eventID+=1
			yield observationPeriod

def main(args):

	# get things ready
	print "Starting at {0}...\n".format(datetime.datetime.now())

	#limitEventTypes = LIMIT_EVENT_TYPES
	limitEventTypes = []
	iterator = wholeCsvIterator(limit=100, limitEventTypes=limitEventTypes, printevery=10000)
	tpattern = TPattern.TPattern(limitEventTypes=limitEventTypes, limitInfrequent=(iterator,10))
	
	
	#dbCursor = establishDBconnection(DB)
	#tpattern.dbCursor = dbCursor
	#tpattern.setDB(DB, TABLE, dbCursor, EVENT_TYPE_COLUMN)

	potentialPatternsChecked = 0
	while tpattern.candidatePatterns:

		potentialPatternsChecked+=1
		candidatePattern = tpattern.candidatePatterns[0]
		del tpattern.candidatePatterns[0]
		print candidatePattern

		countA = 0 
		intervals = []

		for observationPeriod in wholeCsvIterator(limit=100, limitEventTypes=limitEventTypes, printevery=10000):
		#for observationPeriod in testEventsIterator(tpattern):
			tmpcountA, tmpintervals = tpattern.processObservationPeriod(observationPeriod, candidatePattern)
			countA += tmpcountA
			for interval in tmpintervals:
				bisect.insort(intervals, interval)

		tpattern.lookForCriticalIntervals(intervals, countA, candidatePattern, p_value_threshold=.001)

		#sys.exit()




	print "\nChecked {0} patterns.".format(potentialPatternsChecked)

	tpattern.completenessCompetition(tpattern.t_patterns_found)

	print "\nFinished at {0}!".format(datetime.datetime.now())


if __name__ == "__main__":
	main(sys.argv)