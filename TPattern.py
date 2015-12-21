import bisect
import sys
import math
from scipy.stats import binom, norm

class TPattern(object):
	def __init__(self):
		self.eventTypes = {}
		self.t_patterns_found = []
		self.eventTypeCounts = {}
		self.totalTime = 0
		self.distributionsProcessed = set()
		self.eventTypeCounts = {}
		self.eventDistributions = {}

		import shelve
		self.eventDistributions = shelve.open('eventDistributions.so', writeback=True)

		# query = "SELECT DISTINCT {0} \
		# 		 FROM {1} \
		# 		 ORDER BY {0} ASC".format(self.eventTypeColumn, self.tableName)

		# self.dbCursor.execute(query)
		# for eventtype in self.dbCursor.fetchall():
		# 	self.eventTypes.append( EventType(name=eventtype[0]) )
		#self.eventTypes = map(lambda x: EventType(name=x), ['A','B','C','D','E'])

	def setDB(self, dbName, table, dbCursor, eventTypeColumn):
		self.dbName = dbName
		self.tableName = table
		self.dbCursor = dbCursor
		self.eventTypeColumn = eventTypeColumn

	def processObservationPeriod(self, observationPeriod):
		"""For one observation period, add N_b,T,interval for all event-type pairs to the appropriate distribution 
		in the TPattern object. For each eventA, the next instance of each event type is considered an interval.
		For each eventB, it is only considered an interval for the closest instance of each eventA."""
		
		eventTypeCounts, totalTime = self.totalTimeAndEventCountsForObservationPeriod(observationPeriod)


		# iterate over each event instance in reverse, to make sure you only consider the closest instance of eventA to eventB
		eventPairsSeen = {}
		for i in xrange(len(observationPeriod)-1, -1, -1):
			event_instance1 = observationPeriod[i]

			self.eventTypes[str(event_instance1.event_type)] = event_instance1.event_type

			# iterate over each later event instance
			for event_instance2 in observationPeriod[i+1:]:
				if event_instance1.last_timestamp >= event_instance2.first_timestamp:
					continue

				# check if these form a new event-type pair, or if it's the first time this instance has been used as an eventA
				# for this event-type pair and it's the first time this instance of eventB has been used for this event-type pair
				event_type = EventType(first_event_type=event_instance1.event_type, last_event_type=event_instance2.event_type)

				if event_type not in eventPairsSeen or not any( [event_instance1.last_timestamp==eventA or event_instance2.first_timestamp==eventB for eventA,eventB in eventPairsSeen[event_type]] ):

					# get timedelta in between events in hours
					timedelta = event_instance2.first_timestamp - event_instance1.last_timestamp
					timedelta = int(timedelta.total_seconds()/(60*60))
					interval = Interval(timedelta, event_instance1.id, event_instance2.id, eventTypeCounts[event_instance2.event_type], totalTime)

					if str(event_type) in self.eventTypes:
						bisect.insort(self.eventDistributions[str(event_type)], interval)
					else:
						self.eventDistributions[str(event_type)] = [interval]
						self.eventTypes[str(event_type)] = event_type

				

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
		event_types_found = []
		distributionsCheckedForIntervals = set()
		found_at_least_one_tpattern = True
		while found_at_least_one_tpattern:
			found_at_least_one_tpattern = False
			count_of_patterns_found = len(event_types_found)
			
			print "\n" + "-"*50 + "\nStarting loop"

			for event_type_string in self.eventTypes:
				if self.eventTypes[event_type_string].baseEventType:
					continue

				event_type = self.eventTypes[event_type_string]
				
				if event_type in self.distributionsProcessed:
					continue
				else:
					self.distributionsProcessed.add(event_type)

				print "Processing distribution {0}".format(event_type)

				N_a, criticalInterval = self.lookForCriticalIntervals(event_type)
				if criticalInterval:
					e = EventType(first_event_type=event_type.first_event_type, last_event_type=event_type.last_event_type, critical_interval=criticalInterval)
					self.eventTypeCounts[e] = N_a
					self.eventTypes[str(e)] = e
					event_types_found.append(e)

					print "Found tpattern {0}".format(e)

			print "Finished looking for distributions. Found: {0}".format(len(event_types_found) - count_of_patterns_found)

			newDistributions = []
			for event_type_to_add in event_types_found[count_of_patterns_found:]:
				print "Looking for candidate event types to process next time for: {0}".format(event_type_to_add)
				for event_type2_string in self.eventTypes:

					event_type2 = self.eventTypes[str(event_type2_string)]

					if event_type2.baseEventType:
						continue

					if (event_type_to_add,event_type2) in distributionsCheckedForIntervals:
						continue
					else:
						found_at_least_one_tpattern = True
						distributionsCheckedForIntervals.add((event_type_to_add,event_type2))

					# consider time from new event type to the next of each other event type
					if event_type_to_add.last_event_type==event_type2.first_event_type:
						e2 = EventType(first_event_type=event_type_to_add, last_event_type=event_type2.last_event_type)
						newDistribution = self.createDistributionForNewEventType(event_type_to_add, event_type_to_add.critical_interval, event_type2, 'first')
						if newDistribution:
							#print "Will look next time for: {0}".format(e2)
							newDistributions.append( (e2,newDistribution) )

					# consider time from each other event type to the next new event type 
					elif event_type2.last_event_type==event_type_to_add.first_event_type:
						e2 = EventType(first_event_type=event_type2.first_event_type, last_event_type=event_type_to_add)
						newDistribution = self.createDistributionForNewEventType(event_type_to_add, event_type_to_add.critical_interval, event_type2, 'last')
						if newDistribution:
							#print "Will look next time at: {0}, {1}".format(e2, newDistribution)
							newDistributions.append( (e2,newDistribution) )


			for e2, newDistribution in newDistributions:
				self.eventDistributions[str(e2)] = newDistribution
				self.eventTypes[str(e2)] = e2
			
			print "Done processing this round of distributions.\n" + "-"*50 + "\n"


		return event_types_found

	def lookForCriticalIntervals(self, event_type):
		""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
		both distributions according to t-pattern algorithm."""

		AnextB = self.eventDistributions[str(event_type)]
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
				#p_value = 1 - binom.cdf(max(0,N_ab-1), len(intervals), prob)

				
				if p_value < .005:
					#print "\nDistribution for {0}".format(event_type)
					#print "N_a: {0}, N_ab: {1}, Critical interval: ({2}), P(success): {3:.4f}, p_value: {4:.4f}\n".format(N_a, N_ab, str((d1,d2)), prob, p_value)
					return N_ab, (d1, d2)

		return None, None

	def createDistributionForNewEventType(self, event_type, criticalInterval, event_type2, new_type_first_or_last):
		intervals = []
		for interval1 in self.eventDistributions[str(event_type)]:
			if interval1.timedelta>=criticalInterval[0] and interval1.timedelta<=criticalInterval[1]:
				for interval2 in self.eventDistributions[str(event_type2)]:

					# considering candidate event types where the new event type is first
					if new_type_first_or_last == 'first' and interval1.last_event_id == interval2.first_event_id:
						new_interval = Interval(interval2.timedelta, interval1.first_event_id, interval2.last_event_id, interval2.N_b, interval2.T)
						bisect.insort(intervals, new_interval)

					# considering candidate event types where the new event type is last
					if new_type_first_or_last == 'last' and interval2.last_event_id == interval1.first_event_id:
						new_interval = Interval(interval2.timedelta, interval2.first_event_id, interval1.last_event_id, interval1.N_b, interval2.T)
						bisect.insort(intervals, new_interval)
						

		return intervals

	def completenessCompetition(self, eventTypeList):
		completePatterns = []
		print

		for i in xrange(len(eventTypeList)):
			event_type = eventTypeList[i]

			if event_type.baseEventType:
				continue

			other_events = filter(lambda x: x!=event_type and not x.baseEventType, eventTypeList)

			if any(map(lambda x: event_type in x, other_events)):
				print "Considering completeness of {0}: Not complete".format(event_type)
				pass
			else:
				if any(map(lambda x: event_type.getListOfEvents()==x.getListOfEvents(), other_events)):
					iAmLowest = True
					for otherPattern in other_events:
						if event_type.getListOfEvents()==otherPattern.getListOfEvents() and str(event_type)>str(otherPattern):
							print "Considering completeness of {0}: Skipping tie with {1}".format(event_type, otherPattern)
							iAmLowest = False
							break
					if iAmLowest:
						print "Considering completeness of {0}: Complete".format(event_type)
						completePatterns.append(event_type)
				else:
					print "Considering completeness of {0}: Complete".format(event_type)
					completePatterns.append(event_type)

		print "\nComplete patterns:"
		for completePattern in completePatterns:
			print completePattern

class ObservationPeriod(object):
	def __init__(self, events=None):
		if events:
			self.events = events
		else:
			self.events = []

	def __len__(self):

		return len(self.events)

	def __iter__(self):

		return iter(self.events)

	def __getitem__(self, i):

		return self.events[i]

	def addEvent(self, event):

		self.events.append(event)

class Interval(object):
	def __init__(self, timedelta, first_event_id, last_event_id, N_b=None, T=None):
		self.timedelta = timedelta
		self.first_event_id = first_event_id
		self.last_event_id = last_event_id
		self.N_b = N_b
		self.T = T

	def __str__(self):

		return repr(self)
	
	def __repr__(self):

		return "Interval(time:{0},ids:{1},{2}, N_b:{3}, T:{4})".format(self.timedelta, self.first_event_id, self.last_event_id, self.N_b, self.T)
		return "Interval(time:{0},ids:{1},{2})".format(self.timedelta, self.first_event_id, self.last_event_id)

	def __lt__(self, other):
		
		return self.timedelta < other.timedelta

class EventInstance(object):
	def __init__(self, event_type, eventID, first_timestamp, last_timestamp):
		self.event_type = event_type
		self.id = eventID
		self.first_timestamp = first_timestamp
		self.last_timestamp = last_timestamp

	def __str__(self):

		return repr(self)

	def __repr__(self):

		return "{0}".format(self.event_type)

	def __lt__(self, other):

		return self.first_timestamp < other.first_timestamp

class EventType(object):
	def __init__(self, name=None, first_event_type=None, last_event_type=None, critical_interval=None):
		self.first_event_type = first_event_type
		self.last_event_type = last_event_type
		self.critical_interval = critical_interval
		if not name:
			self.name = "({0} {1})".format(self.first_event_type, self.last_event_type)
			self.baseEventType = False
		else:
			self.name = "{0}".format(name)
			self.baseEventType = True

	def __str__(self):

		return repr(self)

	def __repr__(self):
		
		return self.name

	def __eq__(self, other):

		return self.name==other.name

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

		return str(self.name) < str(other.name)

	def getListOfEvents(self):
		events = []
		queue = [self.first_event_type, self.last_event_type]
		while queue:
			node = queue.pop(0)
			if node.baseEventType:
				events.append(node)
			else:
				queue = [node.first_event_type] + [node.last_event_type] + queue
		return events

	def printWithCriticalInterval(self):

		return "({0} {1},{2} {1})".format(self.first_event_type, self.critical_interval[0], self.critical_interval[1], self.last_event_type)

