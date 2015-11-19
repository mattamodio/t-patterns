import bisect
import sys
import math
from scipy.stats import binom, norm

class TPattern(object):
	def __init__(self):
		self.eventTypes = []
		self.dbColumns = []
		self.sub_patterns = []
		self.eventTypeCounts = {}
		self.totalTime = 0

		self.initializeDistributionDict()

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

	def initializeDistributionDict(self):
		"""Make a set of all event types, alphabetically sorted."""
		# query = "SELECT DISTINCT {0} \
		# 		 FROM {1} \
		# 		 ORDER BY {0} ASC".format(self.eventTypeColumn, self.tableName)

		# self.dbCursor.execute(query)

		# for eventtype in self.dbCursor.fetchall():
		# 	self.eventTypes.append(eventtype[0])
		self.eventTypeCounts = {}
		self.eventDistributions = {}
		self.eventTypes = ['A','B','C','D','E']
		for higher_order_pattern in self.sub_patterns:
			bisect.insort(self.eventTypes, str(higher_order_pattern))

		# make sure it's sorted
		self.eventTypes = sorted(self.eventTypes)

		# build skeleton of all event-type pairs
		for i in xrange(len(self.eventTypes)):
			eventtype1 = self.eventTypes[i]
			for eventtype2 in self.eventTypes:
				key = '{0}-{1}'.format(eventtype1,eventtype2)
				self.eventDistributions[key] = []

	def processObservationPeriod(self, observationPeriod):
		"""For one observation period, add N_b,T,interval for all event-type pairs to the appropriate distribution 
		in the TPattern object. For each eventA, the next instance of each event type is considered an interval.
		For each eventB, it is only considered an interval for the closest instance of each eventA."""
		
		self.totalTimeAndEventCountsForObservationPeriod(observationPeriod)

		# iterate over each event instance in reverse, to make sure you only consider the closest instance of eventA to eventB
		eventPairsSeen = {}
		for i in xrange(len(observationPeriod)-1, -1, -1):
			event1 = observationPeriod[i]

			# iterate over each later event instance
			for event2 in observationPeriod[i+1:]:
				if event1.last_timestamp >= event2.first_timestamp:
					continue

				# check if these form a new event-type pair, or if it's the first time this instance has been used as an eventA
				# for this event-type pair and it's the first time this instance of eventB has been used for this event-type pair
				key = '{0}-{1}'.format(event1.eventtype, event2.eventtype)
				if key not in eventPairsSeen or not any( [event1.last_timestamp==eventA or event2.first_timestamp==eventB for eventA,eventB in eventPairsSeen[key]] ):

					# get timedelta in between events in hours
					timedelta = event2.first_timestamp - event1.last_timestamp
					timedelta = int(timedelta.total_seconds()/(60*60))
					interval = Interval(timedelta, self.eventTypeCounts[event2.eventtype], self.totalTime)

					bisect.insort(self.eventDistributions[key], interval)

				

				# track that we've seen this pair, and used this eventA and eventB
				if key in eventPairsSeen:
					eventPairsSeen[key].append((event1.last_timestamp,event2.first_timestamp))
				else:
					eventPairsSeen[key] = [(event1.last_timestamp,event2.first_timestamp)]

	def totalTimeAndEventCountsForObservationPeriod(self, observationPeriod):

		# get total time and event-type counts for this observation period
		firstTime = None
		lastTime = None
		for event in observationPeriod:
			if event.eventtype in self.eventTypeCounts:
				self.eventTypeCounts[event.eventtype] +=1
			else:
				self.eventTypeCounts[event.eventtype] = 1
		
			if not firstTime or event.first_timestamp<firstTime:
				firstTime = event.first_timestamp
			if not lastTime or event.last_timestamp>lastTime:
				lastTime = event.last_timestamp

		totalTime = lastTime - firstTime
		totalTime = int(totalTime.total_seconds()/(60*60))

		self.totalTime += totalTime

	def processDistributions(self):

		found_at_least_one_pattern = False
		for key in self.eventDistributions:

			criticalInterval = self.lookForCriticalIntervals(key)
			if criticalInterval:
				s = Sub_Pattern(key.split('-')[0], key.split('-')[1], criticalInterval)
				if not s in self.sub_patterns:
					self.sub_patterns.append(s)
					found_at_least_one_pattern = True

		return found_at_least_one_pattern

	def lookForCriticalIntervals(self, key):
		""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
		both distributions according to t-pattern algorithm."""

		AnextB = self.eventDistributions[key]
		eventA, eventB = key.split('-')

		for low in xrange(len(AnextB)):
			for high in xrange(len(AnextB)-1, low, -1):

				# print key
				# print AnextB
				
				d1 = AnextB[low].timedelta
				d2 = AnextB[high].timedelta

				# if d1 == d2:
				# 	continue

				
				N_ab = sum( 
						map(lambda pair: all(pair), 
							zip(map(lambda x: d1<=x.timedelta, AnextB), 
								map(lambda x: x.timedelta<=d2, AnextB))))
				N_a = self.eventTypeCounts[eventA]
				N_b = self.eventTypeCounts[eventB]
				T = self.totalTime


				prob = 1 - (1 - N_b / float(T))**(d2-d1+1)

				# the probability of an interval of (d1,d2) after an eventA to contain at least one eventB
				# probs = [(interval.timedelta>=d1 and interval.timedelta<=d2,
				# 		  1 - (1 - float(interval.N_b) / float(interval.T))**(d2-d1+1))
				# 		  for interval in AnextB]
				
				# clt_x = sum([x[0] for x in probs])
				# clt_mean = sum([x[1] for x in probs])
				# clt_variance = sum([x[1]*(1-x[1]) for x in probs])
				# p_value = 1 - norm.cdf( (clt_x-clt_mean)/math.sqrt(clt_variance) )

				# if len(probs)==clt_x:
				# 	p_value = reduce(lambda x,y: x*y, [prob[1] for prob in probs])

				# print probs
				# print clt_x
				# print clt_mean
				# print clt_variance
				# print p_value

				# the probability of observing N_ab intervals with at least one eventB, out of N_a trials
				p_value = 1 - binom.cdf(N_ab, N_a, prob)

				
				if p_value < .05:
					print
					print "Distribution for {0}".format(key)
					# print AnextB
					# print [interval.timedelta for interval in AnextB]
					# print [ (x,round(y,2)) for x,y in probs]
					print "N_a: {0}, N_ab: {1}, Critical interval: ({2}), P(success): {3:.4f}, p_value: {4:.4f}".format(N_a, N_ab, str((d1,d2)), prob, p_value)
					return (d1, d2)


		return None

	def addTpatternsToObservationPeriod(self, observationPeriod):

		events_to_add = []
		
		sub_patterns_added = set()
		anotherLevelExists = True
		while anotherLevelExists:
			anotherLevelExists = False
			for i in xrange(len(observationPeriod)):
				event1 = observationPeriod[i]
				for sub_pattern in self.sub_patterns:
					
					if sub_pattern.first_event_type == event1.eventtype:
						for event2 in observationPeriod[i+1:]:

							if sub_pattern.last_event_type == event2.eventtype and sub_pattern not in sub_patterns_added:
								sub_patterns_added.add(sub_pattern)
								e = Event(str(sub_pattern), event1.first_timestamp, event2.last_timestamp)
								events_to_add.append(e)
								anotherLevelExists = True
		
		for event_to_add in events_to_add:
			bisect.insort(observationPeriod, event_to_add)

		return observationPeriod

	def completenessCompetition(self):

		for i in xrange(len(self.sub_patterns)):
			sub_pattern = self.sub_patterns[i]
			tmp = self.sub_patterns[:i] + self.sub_patterns[i+1:]
			if any(map(lambda x: self.sub_patterns[i] in x, tmp)):
				pass
			else:
				print self.sub_patterns[i]

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


class Event(object):
	def __init__(self, eventtype, first_timestamp, last_timestamp):
		self.eventtype = eventtype
		self.first_timestamp = first_timestamp
		self.last_timestamp = last_timestamp
		self.children = [None, None]

	def __str__(self):

		return repr(self)

	def __repr__(self):

		return "<Event {0} at {1}::{2}>".format(self.eventtype, self.first_timestamp, self.last_timestamp)

	def __lt__(self, other):

		return self.first_timestamp < other.first_timestamp

class Sub_Pattern(object):
	def __init__(self, first_event_type, last_event_type, critical_interval):
		self.first_event_type = first_event_type
		self.last_event_type = last_event_type
		self.critical_interval = critical_interval

	def __str__(self):

		return repr(self)

	def __repr__(self):

		return "({0} {1})".format(self.first_event_type, self.last_event_type)

	def __eq__(self, other):

		return all([self.first_event_type==other.first_event_type, self.last_event_type==other.last_event_type])

	def __contains__(self, item):
		previousIndex = -1
		for event_type in str(item).replace("(", "").replace(")", "").replace(" ", ""):
			index = str(self).replace("(", "").replace(")", "").replace(" ", "")[previousIndex+1:].find(event_type)
			if index == -1:
				return False
			previousIndex = index
		if len(str(self)) == len(str(item)):
			return False
		else:
			return True



