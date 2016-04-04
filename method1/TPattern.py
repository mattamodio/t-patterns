import bisect
import sys
import math
import ast
from scipy.stats import binom, norm

EVENT_TYPES_FILE = 'event_types.txt'

class TPattern(object):
	def __init__(self, limitEventTypes=[], limitInfrequent=None):
		self.eventTypes = []
		self.t_patterns_found = []
		self.totalTime = 0
		self.distributionsProcessed = set()
		self.eventTypeCounts = {}
		self.candidatePatterns = []
		self.limitInfrequent = limitInfrequent

		# query = "SELECT DISTINCT {0} \
		# 		 FROM {1} \
		# 		 ORDER BY {0} ASC".format(self.eventTypeColumn, self.tableName)

		# self.dbCursor.execute(query)
		# for eventtype in self.dbCursor.fetchall():
		# 	self.eventTypes.append( EventType(name=eventtype[0]) )
		


		for event_type_name in ast.literal_eval(open(EVENT_TYPES_FILE).read()):
			if limitEventTypes:
				if event_type_name not in limitEventTypes:
					continue
			self.eventTypes.append(EventType(name=event_type_name))

		if limitInfrequent:
			self.trimInfrequentEvents(limitInfrequent[0], limitInfrequent[1])

		for eventType1 in self.eventTypes:
			for eventType2 in self.eventTypes:
				self.candidatePatterns.append(EventType(first_event_type=eventType1, last_event_type=eventType2))

	def trimInfrequentEvents(self, iterator, threshold):
		for event_type in self.eventTypes:
			self.eventTypeCounts[event_type] = 0

		for observationPeriod in iterator:
			for event in observationPeriod:

				self.eventTypeCounts[event.event_type] +=1

		for k in self.eventTypeCounts:
			if self.eventTypeCounts[k] < threshold:
				self.eventTypes = filter(lambda x: x!=k, self.eventTypes)

	def processObservationPeriod(self, observationPeriod, candidatePattern):
		"""For one observation period, add N_b,T,interval for all event-type pairs to the appropriate distribution 
		in the TPattern object. For each eventA, the next instance of each event type is considered an interval.
		For each eventB, it is only considered an interval for the closest instance of each eventA."""
		totalTime = observationPeriod.events[-1].last_timestamp - observationPeriod.events[0].first_timestamp
		totalTime = int(totalTime.total_seconds()/(60*60))

		if not (candidatePattern.first_event_type.baseEventType and candidatePattern.last_event_type.baseEventType):
			observationPeriod.addEventsForSubpatterns(candidatePattern)


		observationPeriod.events = filter(lambda x: x.event_type in [candidatePattern.first_event_type, candidatePattern.last_event_type], observationPeriod.events)

		# for e in observationPeriod.events:
		# 	print e, e.first_timestamp
		# print

		intervals = []
		countA = 0
		countB = 0
		# iterate over each event instance in reverse, to make sure you only consider the closest instance of eventA to eventB
		laterEventInstancesUsed = set()
		for i in xrange(len(observationPeriod)-1, -1, -1):


			event_instance1 = observationPeriod[i]
			if event_instance1.event_type == candidatePattern.first_event_type:
				countA+=1
			# if event_instance1.event_type == candidatePattern.last_event_type:
			# 	countB+=1
			if event_instance1.event_type != candidatePattern.first_event_type:
				continue

			# iterate over each later event instance
			found_one = False
			for event_instance2 in observationPeriod[i+1:]:
				if not event_instance2.event_type == candidatePattern.last_event_type:
					continue
				if event_instance1.last_timestamp >= event_instance2.first_timestamp:
					continue
				
				# get timedelta in between events in hours
				timedelta = event_instance2.first_timestamp - event_instance1.last_timestamp
				timedelta = int(timedelta.total_seconds()/(60*60))+1
				if timedelta > 24:
					break

				if not found_one and not event_instance2 in laterEventInstancesUsed:
					

					laterEventInstancesUsed.add(event_instance2)
					found_one = True
					
					#interval = Interval(timedelta, event_instance1.id, event_instance2.id, eventTypeCounts[event_instance2.event_type], totalTime)

					bisect.insort(intervals, timedelta)

				countB+=1



		#intervals = map(lambda x: Interval(x, countB, totalTime), intervals)
		intervals = map(lambda x: Interval(x, countB, countA*24), intervals)

		return countA, intervals

	def lookForCriticalIntervals(self, listOfIntervals, N_a, candidatePattern, p_value_threshold):
		""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
		both distributions according to t-pattern algorithm."""

		d1 = 0
		low = 0
		d2 = None

		for high in xrange(len(listOfIntervals)-1, low-1, -1):
			
			
			if listOfIntervals[high].timedelta == d2:
				continue
			else:
				d2 = listOfIntervals[high].timedelta

			#print "Checking {0}, {1}".format(d1, d2)

			if d1==d2: continue

			# the probability of an interval of (d1,d2) after an eventA to contain at least one eventB
			intervals = [(interval.timedelta>=d1 and interval.timedelta<=d2,
					  1 - (1 - float(interval.N_b) / float(interval.T))**(d2-d1+1))
					  for interval in listOfIntervals]

			prob = sum([interval[1] for interval in intervals]) / len(intervals)
			N_ab = sum([interval[0] for interval in intervals])

			# the probability of observing at least N_ab intervals with at least one eventB, out of N_a trials
			p_value = 1 - binom.cdf(max(0,N_ab-1), N_a, prob)

			
			if p_value < p_value_threshold:
				if self.limitInfrequent:
					if N_ab < self.limitInfrequent[1]:
						print "Not enough events to make this a new event type: {0}, Threshold: {1}, Count: {2}".format(candidatePattern, self.limitInfrequent[1], N_ab)
						return None, None
				self.addNewCandidatePatterns(candidatePattern, (d1,d2))
				print "Distribution for {0}".format(candidatePattern)
				print "N_a: {0}, N_ab: {1}, Critical interval: ({2}), P(success): {3:.4f}, p_value: {4:.4f}\n".format(N_a, N_ab, str((d1,d2)), prob, p_value)
				return N_ab, (d1, d2)

		return None, None

	def addNewCandidatePatterns(self, pattern, critical_interval):
		pattern.critical_interval = critical_interval
		self.eventTypes.append(pattern)
		self.t_patterns_found.append(pattern)

		for eventType in self.eventTypes:
			newPattern1 = EventType(first_event_type=pattern, last_event_type=eventType)
			newPattern2 = EventType(first_event_type=eventType, last_event_type=pattern)
			self.candidatePatterns.append(newPattern1)
			self.candidatePatterns.append(newPattern2)

	def completenessCompetition(self, eventTypeList):
		completePatterns = []

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



	# def setDB(self, dbName, table, dbCursor, eventTypeColumn):
		# self.dbName = dbName
		# self.tableName = table
		# self.dbCursor = dbCursor
		# self.eventTypeColumn = eventTypeColumn

	# def createDistributionForNewEventType(self, event_type, criticalInterval, event_type2, new_type_first_or_last):
		# intervals = []
		# for interval1 in self.eventDistributions[str(event_type)]:
		# 	if interval1.timedelta>=criticalInterval[0] and interval1.timedelta<=criticalInterval[1]:
		# 		for interval2 in self.eventDistributions[str(event_type2)]:
		# 			# considering candidate event types where the new event type is first
		# 			if new_type_first_or_last == 'first' and interval1.last_event_id == interval2.first_event_id:
		# 				new_interval = Interval(interval2.timedelta, interval1.first_event_id, interval2.last_event_id, interval2.N_b, interval2.T)
		# 				bisect.insort(intervals, new_interval)

		# 			# considering candidate event types where the new event type is last
		# 			if new_type_first_or_last == 'last' and interval2.last_event_id == interval1.first_event_id:
		# 				new_interval = Interval(interval2.timedelta, interval2.first_event_id, interval1.last_event_id, interval1.N_b, interval2.T)
		# 				bisect.insort(intervals, new_interval)
		# return intervals

	# def processDistributions(self):
		# event_types_found = []
		# distributionsCheckedForIntervals = set()
		# found_at_least_one_tpattern = True
		# while found_at_least_one_tpattern:
		# 	found_at_least_one_tpattern = False
		# 	count_of_patterns_found = len(event_types_found)
			
		# 	print "\n" + "-"*50 + "\nStarting loop"

		# 	for event_type_string in self.eventTypes:
		# 		if self.eventTypes[event_type_string].baseEventType:
		# 			continue

		# 		event_type = self.eventTypes[event_type_string]
				
		# 		if event_type in self.distributionsProcessed:
		# 			continue
		# 		else:
		# 			self.distributionsProcessed.add(event_type)

		# 		print "Processing distribution {0}".format(event_type)

		# 		N_a, criticalInterval = self.lookForCriticalIntervals(event_type)
		# 		if criticalInterval:
		# 			e = EventType(first_event_type=event_type.first_event_type, last_event_type=event_type.last_event_type, critical_interval=criticalInterval)
		# 			self.eventTypeCounts[e] = N_a
		# 			self.eventTypes[str(e)] = e
		# 			event_types_found.append(e)

		# 			print "Found tpattern {0}".format(e)

		# 	print "Finished looking for distributions. Found: {0}".format(len(event_types_found) - count_of_patterns_found)

		# 	newDistributions = []
		# 	for event_type_to_add in event_types_found[count_of_patterns_found:]:
		# 		#print "Looking for candidate event types to process next time for: {0}".format(event_type_to_add)
		# 		for event_type2_string in self.eventTypes:

		# 			event_type2 = self.eventTypes[str(event_type2_string)]

		# 			if event_type2.baseEventType:
		# 				continue

		# 			if (event_type_to_add,event_type2) in distributionsCheckedForIntervals:
		# 				continue
		# 			else:
		# 				found_at_least_one_tpattern = True
		# 				distributionsCheckedForIntervals.add((event_type_to_add,event_type2))

		# 			# consider time from new event type to the next of each other event type
		# 			if event_type_to_add.last_event_type==event_type2.first_event_type:
		# 				e2 = EventType(first_event_type=event_type_to_add, last_event_type=event_type2.last_event_type)
		# 				newDistribution = self.createDistributionForNewEventType(event_type_to_add, event_type_to_add.critical_interval, event_type2, 'first')
		# 				if newDistribution:
		# 					#print "Will look next time for: {0}".format(e2)
		# 					newDistributions.append( (e2,newDistribution) )

		# 			# consider time from each other event type to the next new event type 
		# 			elif event_type2.last_event_type==event_type_to_add.first_event_type:
		# 				e2 = EventType(first_event_type=event_type2.first_event_type, last_event_type=event_type_to_add)
		# 				newDistribution = self.createDistributionForNewEventType(event_type_to_add, event_type_to_add.critical_interval, event_type2, 'last')
		# 				if newDistribution:
		# 					#print "Will look next time at: {0}, {1}".format(e2, newDistribution)
		# 					newDistributions.append( (e2,newDistribution) )


		# 	for e2, newDistribution in newDistributions:
		# 		self.eventDistributions[str(e2)] = newDistribution
		# 		self.eventTypes[str(e2)] = e2
			
		# 	print "Done processing this round of distributions.\n" + "-"*50 + "\n"


		# return event_types_found

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

		bisect.insort(self.events, event)

	def addEventsForSubpatterns(self, pattern):
		events_to_add = []

		if not pattern.first_event_type.baseEventType:
			self.addEventsForSubpatterns(pattern.first_event_type)

		if not pattern.last_event_type.baseEventType:
			self.addEventsForSubpatterns(pattern.last_event_type)

		if not pattern.critical_interval: return None

		for i in xrange(len(self.events)):
			
			event1 = self.events[i]
			if not event1.event_type == pattern.first_event_type: continue

			for j in xrange(i+1, len(self.events)):
				event2 = self.events[j]
				if not event2.event_type == pattern.last_event_type: continue

				
				td = (event2.first_timestamp - event1.last_timestamp)
				td = int(td.total_seconds() / (60.*60.))

				if td > pattern.critical_interval[0] and td < pattern.critical_interval[1]:
					e = EventInstance(event_type=pattern, eventID=None, first_timestamp=event1.first_timestamp, last_timestamp=event2.last_timestamp)
					events_to_add.append(e)
					break

		for e in events_to_add:
			bisect.insort(self.events, e)


class Interval(object):
	def __init__(self, timedelta, N_b=None, T=None):
		self.timedelta = timedelta
		self.N_b = N_b
		self.T = T

	def __str__(self):

		return repr(self)
	
	def __repr__(self):

		return "Interval(time:{0}, N_b:{1}, T:{2})".format(self.timedelta, self.N_b, self.T)

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

	def __ne__(self, other):

		return not (self==other)

	def __contains__(self, item):


		myEvents = self.getListOfEvents()
		othersEvents = item.getListOfEvents()

		if myEvents==othersEvents:
			return False

		j=0
		for otherEvent in othersEvents:
			if otherEvent in myEvents[j:]:
				j+= myEvents.index(otherEvent)+1
			else:
				return False

		if j+1<len(myEvents):
			return False
		else:
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
		if self.critical_interval:
			return "({0} {1},{2} {1})".format(self.first_event_type, self.critical_interval[0], self.critical_interval[1], self.last_event_type)
		else:
			return str(self)

