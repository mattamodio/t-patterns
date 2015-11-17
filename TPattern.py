import bisect
import sys
import math
from scipy.stats import binom, norm

class TPattern(object):
	def __init__(self):
		self.eventTypes = []
		self.dbColumns = []
		self.eventDistributions = {}
		self.tpatterns = []
		self.eventTypeCounts = {}
		self.totalTime = 0

		self.getEventTypes()

	def setDB(self, dbName, table, dbCursor, eventTypeColumn):
		self.dbName = dbName
		self.tableName = table
		self.dbCursor = dbCursor
		self.eventTypeColumn = eventTypeColumn
		self.getColumnNames()

	def getColumnNames(self):
		"""Query the DB for column names and set self.dbColumns as a list containing them."""

		query = "SELECT * \
				 FROM `INFORMATION_SCHEMA`.`COLUMNS` \
				 WHERE `TABLE_SCHEMA`='{0}' AND `TABLE_NAME`='{1}';".format(self.dbName, self.tableName)

		self.dbCursor.execute(query)

		self.dbColumns = [result[3] for result in self.dbCursor.fetchall()]

	def getEventTypes(self):
		"""Make a set of all event types, alphabetically sorted."""
		# query = "SELECT DISTINCT {0} \
		# 		 FROM {1} \
		# 		 ORDER BY {0} ASC".format(self.eventTypeColumn, self.tableName)

		# self.dbCursor.execute(query)

		# for eventtype in self.dbCursor.fetchall():
		# 	self.eventTypes.append(eventtype[0])

		self.eventTypes = ['A','B','C','D','E']
		# make sure it's sorted
		self.eventTypes = sorted(self.eventTypes)

		# build skeleton of all event-type pairs
		for i in xrange(len(self.eventTypes)):
			eventtype1 = self.eventTypes[i]
			for eventtype2 in self.eventTypes:
				key = '{0}-{1}'.format(eventtype1,eventtype2)
				self.eventDistributions[key] = {}
				self.eventDistributions[key]['AnextB'] = []
				self.eventDistributions[key]['AlaterB'] = []

		# build skeleton for counts
		for i in self.eventTypes:
			self.eventTypeCounts[i] = 0

	def buildDistributions(self, events):
		"""For one observation period, add N_b,T,interval for all event-type pairs to the appropriate distribution 
		in the TPattern object. For each eventA, the next instance of each event type is considered an interval.
		For each eventB, it is only considered an interval for the closest instance of each eventA."""
		

		# get total time and event-type counts for this observation period
		firstTime = None
		lastTime = None
		eventTypeCounts = {}
		for event in events:
			timestamp1, eventtype1 = event
			if eventtype1 in eventTypeCounts:
				eventTypeCounts[eventtype1] +=1
			else:
				eventTypeCounts[eventtype1] = 1
		
			if not firstTime or timestamp1<firstTime:
				firstTime = timestamp1
			if not lastTime or timestamp1>lastTime:
				lastTime = timestamp1

		totalTime = lastTime - firstTime
		totalTime = int(totalTime.total_seconds()/(60*60))



		# iterate over each event instance in reverse, to make sure you only consider the closest instance of eventA to eventB
		eventPairsSeen = {}
		for i in xrange(len(events)-1, -1, -1):
			event1 = events[i]
			timestamp1, eventtype1 = event1

			# iterate over each later event instance
			for event2 in events[i+1:]:
				timestamp2,eventtype2 = event2

				# check if these form a new event-type pair, or if it's the first time this instance has been used as an eventA
				# for this event-type pair and it's the first time this instance of eventB has been used for this event-type pair
				key = '{0}-{1}'.format(eventtype1, eventtype2)
				if key not in eventPairsSeen or not any( [timestamp1==eventA or timestamp2==eventB for eventA,eventB in eventPairsSeen[key]] ):

					# get timedelta in between events in hours
					timedelta = timestamp2 - timestamp1
					timedelta = int(timedelta.total_seconds()/(60*60))
					interval = Interval(timedelta, eventTypeCounts[eventtype2], totalTime)

					bisect.insort(self.eventDistributions[key]['AnextB'], interval)

				

				# track that we've seen this pair, and used this eventA and eventB
				if key in eventPairsSeen:
					eventPairsSeen[key].append((timestamp1,timestamp2))
				else:
					eventPairsSeen[key] = [(timestamp1,timestamp2)]

			

	def processDistributions(self):
		for key in self.eventDistributions:

			criticalInterval = self.lookForCriticalIntervals(key)
			if criticalInterval:
				self.tpatterns.append( (key,criticalInterval) )
		print
		print "{0}/{1} intervals have t-patterns.".format(len(self.tpatterns), len(self.eventDistributions))

	def lookForCriticalIntervals(self, key):
		""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
		both distributions according to t-pattern algorithm."""

		AnextB = self.eventDistributions[key]['AnextB']

		for low in xrange(len(AnextB)):
			for high in xrange(len(AnextB)-1, low, -1):

				# print key
				# print AnextB
				
				d1 = AnextB[low].timedelta
				d2 = AnextB[high].timedelta

				if d1 == d2:
					continue

				
				# N_ab = sum( 
				# 		map(lambda pair: all(pair), 
				# 			zip(map(lambda x: d1<=x.timedelta, AnextB), 
				# 				map(lambda x: x.timedelta<=d2, AnextB))))
				

				# the probability of an interval of (d1,d2) after an eventA to contain at least one eventB
				probs = [(interval.timedelta>=d1 and interval.timedelta<=d2,
						  1 - (1 - float(interval.N_b) / float(interval.T))**(d2-d1+1))
						  for interval in AnextB]
				
				clt_x = sum([x[0] for x in probs])
				clt_mean = sum([x[1] for x in probs])
				clt_variance = sum([x[1]*(1-x[1]) for x in probs])
				p_value = 1 - norm.cdf( (clt_x-clt_mean)/math.sqrt(clt_variance) )

				if len(probs)==clt_x:
					p_value = reduce(lambda x,y: x*y, [prob[1] for prob in probs])

				# print probs
				# print clt_x
				# print clt_mean
				# print clt_variance
				# print p_value

				# the probability of observing N_ab intervals with at least one eventB, out of N_a trials
				# p_value = 1 - binom.cdf(N_ab, N_a, prob)

				
				if p_value < .05:
					print
					print "Distribution for {0}".format(key)
					print AnextB
					print [interval.timedelta for interval in AnextB]
					print [ (x,round(y,2)) for x,y in probs]
					print "N_a: {0}, N_ab: {1}, p_value: {2}".format(len(probs), clt_x, p_value)
					print "Critical interval (d1,d2): ({0}, {1})".format(d1, d2, p_value)
					return (d1, d2)


		return None




class Interval(object):
	def __init__(self, timedelta, N_b, T):
		self.timedelta = timedelta
		self.N_b = N_b
		self.T = T

	def __str__(self):
		return repr(self)
	def __repr__(self):
		return "Interval({0},{1},{2})".format(self.timedelta, self.N_b, self.T)

	def __lt__(self, other):
		return self.timedelta < other.timedelta



