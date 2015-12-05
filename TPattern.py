import bisect
import sys
import math
from scipy.stats import binom, norm

class TPattern(object):
	def __init__(self):
		self.eventTypes = []
		self.dbColumns = []
		self.t_patterns_found = []
		self.eventTypeCounts = {}
		self.totalTime = 0
		self.distributionsProcessed = set()

		# query = "SELECT DISTINCT {0} \
		# 		 FROM {1} \
		# 		 ORDER BY {0} ASC".format(self.eventTypeColumn, self.tableName)

		# self.dbCursor.execute(query)
		# for eventtype in self.dbCursor.fetchall():
		# 	self.eventTypes.append( EventType(eventtype[0],eventtype[0]) )
		self.eventTypes = map(lambda x: EventType(x,x), ['A','B','C','D','E'])

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
		
		self.eventTypeCounts = {}
		self.eventDistributions = {}

	def processObservationPeriod(self, observationPeriod):
		"""For one observation period, add N_b,T,interval for all event-type pairs to the appropriate distribution 
		in the TPattern object. For each eventA, the next instance of each event type is considered an interval.
		For each eventB, it is only considered an interval for the closest instance of each eventA."""
		
		eventTypeCounts, totalTime = self.totalTimeAndEventCountsForObservationPeriod(observationPeriod)


		# iterate over each event instance in reverse, to make sure you only consider the closest instance of eventA to eventB
		eventPairsSeen = {}
		for i in xrange(len(observationPeriod)-1, -1, -1):
			event_instance1 = observationPeriod[i]

			# iterate over each later event instance
			for event_instance2 in observationPeriod[i+1:]:
				if event_instance1.last_timestamp >= event_instance2.first_timestamp:
					continue

				# check if these form a new event-type pair, or if it's the first time this instance has been used as an eventA
				# for this event-type pair and it's the first time this instance of eventB has been used for this event-type pair
				event_type = EventType(event_instance1.event_type, event_instance2.event_type)
				if event_type in self.distributionsProcessed:
					continue

				if event_type not in eventPairsSeen or not any( [event_instance1.last_timestamp==eventA or event_instance2.first_timestamp==eventB for eventA,eventB in eventPairsSeen[event_type]] ):

					# get timedelta in between events in hours
					timedelta = event_instance2.first_timestamp - event_instance1.last_timestamp
					timedelta = int(timedelta.total_seconds()/(60*60))
					interval = Interval(timedelta, eventTypeCounts[event_instance2.event_type], totalTime)

					if event_type in self.eventDistributions:
						bisect.insort(self.eventDistributions[event_type], interval)
					else:
						self.eventDistributions[event_type] = [interval]

				

				# track that we've seen this pair, and used this eventA and eventB
				if event_type in eventPairsSeen:
					eventPairsSeen[event_type].append((event_instance1.last_timestamp,event_instance2.first_timestamp))
				else:
					eventPairsSeen[event_type] = [(event_instance1.last_timestamp,event_instance2.first_timestamp)]

	def totalTimeAndEventCountsForObservationPeriod(self, observationPeriod):

		# get total time and event-type counts for this observation period
		firstTime = None
		lastTime = None
		eventTypeCounts = {}
		for event_instance in observationPeriod:
			if event_instance.event_type in eventTypeCounts:
				eventTypeCounts[event_instance.event_type] +=1
			else:
				eventTypeCounts[event_instance.event_type] = 1
		
			if event_instance.event_type in self.eventTypeCounts:
				self.eventTypeCounts[event_instance.event_type] +=1
			else:
				self.eventTypeCounts[event_instance.event_type] = 1


			if not firstTime or event_instance.first_timestamp<firstTime:
				firstTime = event_instance.first_timestamp
			if not lastTime or event_instance.last_timestamp>lastTime:
				lastTime = event_instance.last_timestamp

		totalTime = lastTime - firstTime
		totalTime = int(totalTime.total_seconds()/(60*60))

		return eventTypeCounts, totalTime

	def processDistributions(self):

		found_at_least_one_pattern = False
		for event_type in self.eventDistributions:
			if event_type in self.distributionsProcessed:
				continue
			self.distributionsProcessed.add(event_type)

			print "Processing distribution {0}".format(event_type)

			criticalInterval = self.lookForCriticalIntervals(event_type)
			if criticalInterval:
				e = EventType(event_type.first_event_type, event_type.last_event_type, criticalInterval)
				if not e in self.t_patterns_found:
					self.t_patterns_found.append(e)
					found_at_least_one_pattern = True

					self.eventTypes.append(e)

		self.initializeDistributionDict()

		return found_at_least_one_pattern

	def lookForCriticalIntervals(self, event_type):
		""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
		both distributions according to t-pattern algorithm."""

		AnextB = self.eventDistributions[event_type]
		eventA = event_type.first_event_type
		eventB = event_type.last_event_type

		N_a = self.eventTypeCounts[eventA]
		#N_b = self.eventTypeCounts[eventB]
		T = self.totalTime

		for low in xrange(len(AnextB)):
			for high in xrange(len(AnextB)-1, low-1, -1):
				
				d1 = AnextB[low].timedelta
				d2 = AnextB[high].timedelta

				# N_ab = sum( 
				# 		map(lambda pair: all(pair), 
				# 			zip(map(lambda x: d1<=x.timedelta, AnextB), 
				# 				map(lambda x: x.timedelta<=d2, AnextB))))
				


				#prob = 1 - (1 - N_b / float(T))**(d2-d1+1)

				# the probability of an interval of (d1,d2) after an eventA to contain at least one eventB
				intervals = [(interval.timedelta>=d1 and interval.timedelta<=d2,
						  1 - (1 - float(interval.N_b) / float(interval.T))**(d2-d1+1))
						  for interval in AnextB]
				
				prob = sum([p[1] for p in intervals]) / len(intervals)
				N_ab = sum([interval[0] for interval in intervals])

				# the probability of observing at least N_ab intervals with at least one eventB, out of N_a trials
				p_value = 1 - binom.cdf(max(0,N_ab-1), N_a, prob)

				
				if p_value < .005:
					#print "\nDistribution for {0}".format(event_type)
					#print "N_a: {0}, N_ab: {1}, Critical interval: ({2}), P(success): {3:.4f}, p_value: {4:.4f}\n".format(N_a, N_ab, str((d1,d2)), prob, p_value)
					return (d1, d2)

		return None

	def addTpatternsToObservationPeriod(self, observationPeriod):

		events_to_add = []
		
		higher_order_event_types_added = set()
		anotherLevelExists = True
		while anotherLevelExists:
			anotherLevelExists = False
			for i in xrange(len(observationPeriod)):
				event_instance1 = observationPeriod[i]

				for event_type in self.t_patterns_found:
					if not event_type.critical_interval: continue

					if event_type.first_event_type == event_instance1.event_type:

						for event_instance2 in observationPeriod[i+1:]:

							timediff = event_instance2.first_timestamp - event_instance1.first_timestamp
							timediff = timediff.total_seconds()/(60*60)
							if event_type.last_event_type == event_instance2.event_type \
							and event_type not in higher_order_event_types_added \
							and event_type.critical_interval[0] <= timediff \
							and event_type.critical_interval[1] >= timediff:

								higher_order_event_types_added.add(event_type)
								e = EventInstance(event_type, event_instance1.first_timestamp, event_instance2.last_timestamp)
								
								bisect.insort(observationPeriod.events, e)
								anotherLevelExists = True

		return observationPeriod

	def completenessCompetition(self):
		completePatterns = []
		print
		for i in xrange(len(self.t_patterns_found)):
			sub_pattern = self.t_patterns_found[i]
			#print "Considering completeness of {0}".format(sub_pattern)
			tmp = self.t_patterns_found[:i] + self.t_patterns_found[i+1:]

			
			if any(map(lambda x: sub_pattern in x, tmp)):
				#print "Not complete"
				pass
			else:
				if any(map(lambda x: sub_pattern.getListOfEvents()==x.getListOfEvents(), tmp)):
					iAmLowest = True
					for otherPattern in tmp:
						if sub_pattern.getListOfEvents()==otherPattern.getListOfEvents() and str(sub_pattern)>str(otherPattern):
							#print "Skipping tie between {0} and {1}".format(sub_pattern, otherPattern)
							iAmLowest = False
							break
					if iAmLowest:
						#print "Complete"
						completePatterns.append(sub_pattern)
				else:
					#print "Complete"
					completePatterns.append(sub_pattern)

		for completePattern in completePatterns:
			print completePattern

class ObservationPeriod(object):
	def __init__(self, events):

		self.events = events

	def __len__(self):

		return len(self.events)

	def __iter__(self):

		return iter(self.events)

	def __getitem__(self, i):

		return self.events[i]

class Interval(object):
	def __init__(self, timedelta, N_b=None, T=None):
		self.timedelta = timedelta
		self.N_b = N_b
		self.T = T

	def __str__(self):

		return repr(self)
	
	def __repr__(self):

		return "Interval({0},{1},{2})".format(self.timedelta, self.N_b, self.T)

	def __lt__(self, other):
		
		return self.timedelta < other.timedelta

class EventInstance(object):
	def __init__(self, event_type, first_timestamp, last_timestamp):
		self.event_type = event_type
		self.first_timestamp = first_timestamp
		self.last_timestamp = last_timestamp

	def __str__(self):

		return repr(self)

	def __repr__(self):

		return "{0}".format(self.event_type)

	def __lt__(self, other):

		return self.first_timestamp < other.first_timestamp

class EventType(object):
	def __init__(self, first_event_type, last_event_type, critical_interval=None):
		self.first_event_type = first_event_type
		self.last_event_type = last_event_type
		self.critical_interval = critical_interval

	def __str__(self):

		return repr(self)

	def __repr__(self):
		if type(self.first_event_type)==str and type(self.last_event_type)==str:
			return "{0}".format(self.first_event_type)
		elif not self.critical_interval:
			return "({0} {1})".format(self.first_event_type, self.last_event_type)
		else:
			return "({0} {1},{2} {3})".format(self.first_event_type, self.critical_interval[0], self.critical_interval[1], self.last_event_type)

	def __eq__(self, other):


		#return all([self.first_event_type==other.first_event_type, self.last_event_type==other.last_event_type])
		return all([str(self.first_event_type)==str(other.first_event_type), str(self.last_event_type)==str(other.last_event_type)])

	def __contains__(self, item):

		myEvents = self.getListOfEvents()
		othersEvents = item.getListOfEvents()

		if myEvents==othersEvents:
			return False

		j=0
		for otherEvent in othersEvents:
			if otherEvent in myEvents[j:]:
				j+= myEvents.index(otherEvent)
			else:
				return False
		return True

	def __hash__(self):

		return hash(str(self))

	def __lt__(self, other):

		return str(self.first_event_type) + str(self.last_event_type) < str(other.first_event_type) + str(other.last_event_type)

	def getListOfEvents(self):
		events = []
		queue = [self.first_event_type, self.last_event_type]
		while queue:
			vertex = queue.pop(0)
			if isinstance(vertex, str):
				events.append(vertex)
				queue.pop(0)
			else:
				queue = [vertex.first_event_type] + [vertex.last_event_type] + queue
		return events


