import sys
import shelve
import datetime
import MySQLdb
import matplotlib.pyplot as plt
import csv_TPattern
import ast
import bisect
import csv
from csv_TPattern import ObservationPeriod, Interval, EventType, EventInstance

DATAFILE  = '/Users/matthewamodio/Desktop/claim_events_personrows_simple2.csv'

DATA = 'amfam'
#DATA = 'fake'

if DATA=='amfam':
	DB = 'amfam'
	ORIGINAL_TABLE = 'claim_events_simple'
	TABLE = 'tmp_table'
	#reset_table_file = 'create_table_amfam.txt'
	reset_table_file = 'create_table_amfam_with_kl.txt'
elif DATA=='fake':
	DB = 'test'
	ORIGINAL_TABLE = 'fakedata'
	TABLE = 'fakedata2'
	reset_table_file = 'create_table_fakedata.txt'

p_value_threshold = .001
WINDOW = 12
laplace = 1 #prior on N_b per observation periodtmp
KL_CUTOFF = False #2.706 #chi-squared with df=1 and alpha=.10
LIMIT_INFREQUENT_EVENTS = False #false or a support threshold
LIMIT_INFREQUENT_PAIRS = False #false or a support threshold
OUTPUT_DATA_FILE = 'outputdata_with_happiness.csv'

def getCursor(db, table):
	# establish connection to DB
    conn = MySQLdb.connect( host='127.0.0.1', user='root', db=db, charset='utf8')
    conn.autocommit(True)

    cursor = conn.cursor()
    return cursor

def setDB(tpattern, db, table, cursor):
    '''
    Establishes connection to DB, turns on autocommit, and gets all of the column names from the table and puts them
    into the FIELDS_FOUND variable.
    '''

    

    tpattern.DB = db
    tpattern.table = table
    tpattern.dbCursor = cursor

    return tpattern

def getCounts(tpattern):
	
	q = '''
		SELECT observation_period, event_type, COUNT(*)
		FROM {0}
		GROUP BY event_type, observation_period
		'''.format(tpattern.table)
	tpattern.dbCursor.execute(q)
	counts = {}
	for row in tpattern.dbCursor.fetchall():
		observation_period, event_type, count = row
		# note how many observation periods there are
		if observation_period > tpattern.num_observation_periods: tpattern.num_observation_periods = observation_period

		if int(observation_period) not in counts:
			counts[int(observation_period)] = {}
		counts[observation_period][str(event_type)] = int(count)
	return counts

def getQuery(tpattern, candidatePattern, window=False):
	if not window:
		q = '''
		SELECT *
		FROM
		(
		SELECT e.*, d.N_b
		FROM


		(
		SELECT b.observation_period AS op1, COUNT(*) AS N_b
		FROM (
			SELECT observation_period, id
			FROM {0}
			WHERE event_type='{1}'
			) a,
			(
			SELECT observation_period, id
			FROM {0}
			WHERE event_type='{2}'
			) b
		WHERE a.observation_period=b.observation_period
		AND NOT a.id=b.id
		GROUP BY b.observation_period
		) d,

		(
		SELECT a.id AS id1, b.id AS id2, b.time_start-a.time_end AS timediff, b.observation_period, a.observation_period_T
		FROM
			(
			SELECT observation_period, id, time_end, observation_period_T
			FROM {0}
			WHERE event_type='{1}'
			) a,
			(
			SELECT observation_period, id, time_start
			FROM {0}
			WHERE event_type='{2}'
			) b
		WHERE a.observation_period=b.observation_period
		AND NOT a.id=b.id
		AND b.time_start-a.time_end>0
		) e


		WHERE d.op1=e.observation_period
		GROUP BY id1
		ORDER BY id2, timediff ASC
		) f
		GROUP BY id2
		'''.format(tpattern.table, candidatePattern.first_event_type, candidatePattern.last_event_type)

	else:
		q='''
		SELECT *
		FROM
		(
		SELECT e.*, d.N_b
		FROM


		(
		SELECT b.observation_period AS op1, COUNT(*) AS N_b
		FROM (
			SELECT observation_period, id, time_end AS window_begin, time_end+{3} AS window_end
			FROM {0}
			WHERE event_type='{1}'
			) a,
			(
			SELECT observation_period, id, time_start
			FROM {0}
			WHERE event_type='{2}'
			) b
		WHERE a.observation_period=b.observation_period
		AND NOT a.id=b.id
		AND b.time_start>a.window_begin AND b.time_start<a.window_end
		GROUP BY b.observation_period
		) d,

		(
		SELECT a.id AS id1, b.id AS id2, b.time_start-a.time_end AS timediff, b.observation_period, a.observation_period_T
		FROM
			(
			SELECT observation_period, id, time_end, time_end AS window_begin, time_end+{3} AS window_end, observation_period_T
			FROM {0}
			WHERE event_type='{1}'
			) a,
			(
			SELECT observation_period, id, time_start
			FROM {0}
			WHERE event_type='{2}'
			) b
		WHERE a.observation_period=b.observation_period
		AND NOT a.id=b.id
		AND b.time_start>a.window_begin AND b.time_start<a.window_end
		) e


		WHERE d.op1=e.observation_period
		GROUP BY id1
		ORDER BY id2, timediff ASC
		) f
		GROUP BY id2
				'''.format(tpattern.table, candidatePattern.first_event_type, candidatePattern.last_event_type, window)

	return q

def printParameters():
	print '''\
	\nParameters:\
	\nTable:{6}\
	\nLimit Infrequent Events: {0}\
	\nLimit Infrequent Pairs: {1}\
	\nPrune KL-Divergence: {2}\
	\nWindow: {3}\
	\nLaplace Pseudocounts: {4}\
	\nP-value Threshold: {5}\
	\n'''.format(LIMIT_INFREQUENT_EVENTS,LIMIT_INFREQUENT_PAIRS, KL_CUTOFF, WINDOW, laplace, p_value_threshold, ORIGINAL_TABLE)

def main(args):

	# get things ready
	start = datetime.datetime.now()
	print "Starting at {0}...\n".format(start)
	sys.stdout.flush()

	printParameters()

	# drop table and create new data
	# cursor = getCursor(DB, TABLE)
	# with open(reset_table_file) as f:
	# 	for line in f:
	# 		if line.replace('\n', ''):
	# 			line = line.replace('WORKING_TABLE', TABLE)
	# 			line = line.replace('ORIGINAL_TABLE', ORIGINAL_TABLE)
	# 			cursor.execute(line)



	tpattern = csv_TPattern.TPattern(limit_infrequent_events=LIMIT_INFREQUENT_EVENTS, limit_infrequent_pairs=LIMIT_INFREQUENT_PAIRS, window=WINDOW, original_table=ORIGINAL_TABLE, kl_cutoff=KL_CUTOFF)
	
	# tpattern = setDB(tpattern, DB, TABLE, cursor)
	tpattern.datafile = DATAFILE
	tpattern.getEventTypes()
	



	patterns_checked = 0

	
	while tpattern.candidatePatterns:
		patterns_checked +=1
		#counts = getCounts(tpattern)
		candidatePattern = tpattern.candidatePatterns.pop(0)
		print "Found:{0} Checked:{1} Candidates:{2}  {3}".format(len(tpattern.t_patterns_found), patterns_checked, len(tpattern.candidatePatterns), candidatePattern.prettyPrint(tpattern.mapCounterToEventtype))

		
		#q = getQuery(tpattern, candidatePattern, window=WINDOW)
		#tpattern.dbCursor.execute(q)

		intervals = []
		
		observation_periods_seen = set()

		N_a=0
		with open(tpattern.datafile, 'rb') as f:

			for row in f:
				row = ast.literal_eval(row)
				N_b=0
				tmp_intervals = []
				
				for index1,event1 in enumerate(row):
					eventid1,claimnumber1,ts1,event_type1 = event1
					ts1 = datetime.datetime.strptime(ts1, "%Y-%m-%d %H:%M:%S")
					if tpattern.mapCounterToEventtype[candidatePattern.first_event_type.name]==event_type1:
						#print "e1: {0}".format(event_type1)
						N_a+=1
						FOUND_ONE_B=False
						


						for index2,event2 in enumerate(row):
							if FOUND_ONE_B: break
							eventid2,claimnumber2,ts2,event_type2 = event2
							if index2>index1 and tpattern.mapCounterToEventtype[candidatePattern.last_event_type.name]==event_type2:
								#print "e2: {0}".format(event_type2)

								

								ts2 = datetime.datetime.strptime(ts2, "%Y-%m-%d %H:%M:%S")
								timedelta = ts2-ts1
								T = datetime.datetime.strptime(row[-1][2], "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(row[0][2], "%Y-%m-%d %H:%M:%S")

								timedelta = int(timedelta.total_seconds()/(60.*60.))+1
								T = int(T.total_seconds()/(60.*60.))+1

								if timedelta>WINDOW:
									FOUND_ONE_B=True
									continue
								
								N_b +=1
								FOUND_ONE_B=True
								tmp_intervals.append((timedelta, T, eventid1, eventid2, claimnumber1))
								

	
				for tmp_interval in tmp_intervals:
					timedelta, T, first_id, last_id, observation_period = tmp_interval
					interval = Interval(timedelta=timedelta, N_b=int(N_b), T=WINDOW, first_id=first_id, last_id=last_id, observation_period=observation_period, observation_period_T=T)
					intervals.append(interval)
				
			#print len(intervals)

			# 	sys.exit()
			# 	if not WINDOW:
			# 		first_id, last_id, timedelta, observation_period, T, N_b = row
			# 		N_a +=1
			# 		N_b +=laplace

			# 		interval = Interval(timedelta=int(timedelta), N_b=int(N_b), T=T, first_id=first_id, last_id=last_id, observation_period=observation_period, observation_period_T=T)
			# 		intervals.append(interval)

			# 	else:
			# 		first_id, last_id, timedelta, observation_period, T, N_b = row
			# 		N_b +=laplace
			# 		if not observation_period in observation_periods_seen:
			# 			N_a += counts[observation_period][candidatePattern.first_event_type.name]
			# 			observation_periods_seen.add(observation_period)
			# 		window_T = WINDOW*counts[observation_period][candidatePattern.first_event_type.name] 
			# 		interval = Interval(timedelta=int(timedelta), N_b=int(N_b), T=window_T, first_id=first_id, last_id=last_id, observation_period=observation_period, observation_period_T=T)
			# 		intervals.append(interval)

			intervals = sorted(intervals)

			N_ab, critical_interval = tpattern.lookForCriticalIntervals(intervals, N_a, candidatePattern, p_value_threshold)
			
			sys.stdout.flush()
			#sys.exit()



	sys.exit()

	print "\nChecked {0} patterns.".format(patterns_checked)
	sys.stdout.flush()

	tpattern.completenessCompetition(tpattern.t_patterns_found, OUTPUT_DATA_FILE)

	stop = datetime.datetime.now()
	print "\nFinished at {0}!\nTotal Time: {1}".format(stop, stop-start)


if __name__ == "__main__":
	main(sys.argv)