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

import csv

CSV_FILE = '/Users/matthewamodio/Desktop/claim_events.csv'

def wholeCsvIterator(tpattern, limit=None):
	with open(CSV_FILE, 'r') as f:
		reader = csv.reader(f)
		reader.next()

		if limit:
			counter=0

		
		event_id, claimnumber, timestamp, _,eventtype = reader.next()[:5]
		timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
		event = TPattern.EventInstance(TPattern.EventType(name=eventtype), event_id, timestamp, timestamp)
		events = [event]
		prev_claimnumber = claimnumber

		for row in reader:
			event_id, claimnumber, timestamp, _,eventtype = row[:5]
			timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
			event = TPattern.EventInstance(TPattern.EventType(name=eventtype), event_id, timestamp, timestamp)

			if prev_claimnumber==claimnumber:
				events.append(event)
			else:
				observationPeriod = TPattern.ObservationPeriod(events)
				yield observationPeriod
				events = []


				if limit:
					counter+=1
					if counter % 1 == 0: print "Processing observation {0}".format(counter)
					if counter>= limit:
						break

			prev_claimnumber = claimnumber

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

	# query one observation period at a time
	row_count = 1
	while row_count <= numberOfObservationPeriods:
		q = "SELECT id,ts,eventtype FROM {0} WHERE claimnumber={1}".format(TABLE, row_count)
		tpattern.dbCursor.execute(q)
		events = tpattern.dbCursor.fetchall()
		events = map(lambda x: TPattern.EventInstance(TPattern.EventType(name=x[2]), x[0], x[1], x[1]), events)
		observationPeriod = ObservationPeriod(events)
		row_count +=1
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
	
	
	tpattern = TPattern.TPattern()
	#dbCursor = establishDBconnection(DB)
	#tpattern.setDB(DB, TABLE, dbCursor, EVENT_TYPE_COLUMN)

	#print "\nStarting loop, tpatterns found: {0}".format(tpattern.t_patterns_found)


	for observationPeriod in wholeCsvIterator(tpattern, limit=100):
		tpattern.processObservationPeriod(observationPeriod)

	for key in tpattern.t_patterns_found:
		print "{0}: {1}".format(key, tpattern.t_patterns_found[key])
	sys.exit()
	event_types_found = tpattern.processDistributions()
	
	tpattern.completenessCompetition(event_types_found)


	print "\nFinished at {0}!".format(datetime.datetime.now())


if __name__ == "__main__":
	main(sys.argv)