import bisect
import sys
from scipy.stats import binom

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
		"""For one observation period, build all of the distributions of event-type pairs for this unit."""
		
		firstTime = None
		lastTime = None
		
		for i in xrange(len(events)):
			eventPairsSeen = set()
			# add this pair to distribution
			event1 = events[i]
			timestamp1,eventtype1 = event1
			for event2 in events[i+1:]:
				timestamp2,eventtype2 = event2
				key = '{0}-{1}'.format(eventtype1, eventtype2)
				timedelta = timestamp2 - timestamp1
				# convert timedelta to hours
				timedelta = int(timedelta.total_seconds()/(60*60))
				
				if key not in eventPairsSeen:
					bisect.insort(self.eventDistributions[key]['AnextB'],timedelta)
				else:
					bisect.insort(self.eventDistributions[key]['AlaterB'],timedelta)

				eventPairsSeen.add(key)

			# keep track of counts
			self.eventTypeCounts[eventtype1]+=1

			# account for total time
			if not firstTime or timestamp1<firstTime:
				firstTime = timestamp1
			if not lastTime or timestamp1>lastTime:
				lastTime = timestamp1
		totalTime = lastTime - firstTime
		totalTime = int(totalTime.total_seconds()/(60*60))
		self.totalTime += totalTime

	def processDistributions(self):
		self.tpatternsfound=0
		for key in self.eventDistributions:
			eventA,eventB = key.split('-')

			self.lookForCriticalIntervals(eventA, eventB)
		print
		print "{0}/{1} intervals have t-patterns.".format(self.tpatternsfound, len(self.eventDistributions))

	def lookForCriticalIntervals(self, eventA, eventB):
		""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
		both distributions according to t-pattern algorithm."""

		T = self.totalTime
		N_a = self.eventTypeCounts[eventA]
		N_b = self.eventTypeCounts[eventB]
		AnextB = self.eventDistributions['{0}-{1}'.format(eventA,eventB)]['AnextB']
		AlaterB = self.eventDistributions['{0}-{1}'.format(eventA,eventB)]['AlaterB']

		for low in xrange(len(AnextB)):
			for high in xrange(len(AnextB)-1, low, -1):

				d1 = AnextB[low]
				d2 = AnextB[high]

				if d1 == d2:
					continue

				

				N_ab = sum( 
						map(lambda pair: all(pair), 
							zip(map(lambda x: d1<=x, AnextB), 
								map(lambda x: x<=d2, AnextB)))) #\
						# + sum(
						# map(lambda pair: all(pair),
						# 	zip(map(lambda x: d1<=x, AlaterB),
						# 		map(lambda x: x<=d2, AlaterB))))

				

				# the probability of an interval of (d1,d2) after an eventA to contain at least one eventB
				prob = 1 - (1 - float(N_b) / float(T))**(d2-d1+1)

				# the probability of observing N_ab intervals with at least one eventB, out of N_a trials
				p_value = 1 - binom.cdf(N_ab, N_a, prob)

				
				if p_value < .05:
					self.tpatternsfound +=1
					print
					print "Distribution for {0}-{1}".format(eventA,eventB)
					print AnextB
					print "N_a: {0}, N_ab: {1}, prob: {2}, p_value: {3}".format(N_a, N_ab, prob, p_value)
					print "Critical interval (d1,d2): ({0}, {1})".format(d1, d2, p_value)
					return (d1, d2)


		return None








