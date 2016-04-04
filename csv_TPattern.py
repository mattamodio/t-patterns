import bisect
import sys
import math
import ast
import itertools
import csv
from scipy.stats import binom, norm


class TPattern(object):
	def __init__(self, limit_infrequent_events=False, limit_infrequent_pairs=False, window=False, original_table=None, kl_cutoff=None, poisson_exact=False):
		self.eventTypes = []
		self.mapCounterToEventtype= {}
		self.mapEventTypeToCounter = {}
		self.t_patterns_found = []
		self.eventTypeCounts = {}
		self.candidatePatterns = []
		self.num_observation_periods = 0
		self.limit_infrequent_events = limit_infrequent_events
		self.dbCursor = None
		self.DB = None
		self.table = None
		self.original_table = original_table
		self.kl_cutoff = kl_cutoff
		self.poisson_exact = poisson_exact
		self.limit_infrequent_pairs = limit_infrequent_pairs
		self.datafile = None

		if not window:
			self.window=sys.maxint
		else:
			self.window = window
		
	def getEventTypes(self):
		# query for all event types
		# q = ''' SELECT event_type, COUNT(*) AS count
		# 		FROM (SELECT REPLACE(event_type,' ','') AS event_type FROM {0}) a
		# 		GROUP BY event_type
		# 		ORDER BY event_type
		# '''.format(self.original_table)

		# self.dbCursor.execute(q)

		# create event types
		event_types_found = set()
		with open(self.datafile, 'rb') as f:
			num = 0
			for row in f:
				row = ast.literal_eval(row)
				for event in row:
					_,_,_,event_type_name = event

					if event_type_name not in event_types_found:
						event_types_found.add(event_type_name)
						num+=1
						number_id = str(num)

					
						self.mapCounterToEventtype[number_id] = event_type_name
						self.mapEventTypeToCounter[event_type_name] = number_id

						event_type = EventType(name=number_id)
						self.eventTypes.append(event_type)
				
			for eventType1 in self.eventTypes:
				for eventType2 in self.eventTypes:

					# add candidate pattern
					eventType = EventType(first_event_type=eventType1, last_event_type=eventType2)
					self.candidatePatterns.append(eventType)

	def lookForCriticalIntervals(self, listOfIntervals, N_a, candidatePattern, p_value_threshold):
		""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
		both distributions according to t-pattern algorithm."""

		if not listOfIntervals: return None, None

		d1 = None
		d2 = 0

		for low in xrange(-1, len(listOfIntervals)):
			
			if d1==None: #first try an interval starting with 0
				d1 = 0
			elif listOfIntervals[low].timedelta == d1: #pass intervals that are the same as the previous one
				continue
			else: #later try intervals from the list
				d1 = listOfIntervals[low].timedelta

			for high in xrange(len(listOfIntervals)-1, max(low-1,-1), -1):

				if listOfIntervals[high].timedelta > self.window: #pass intervals that are greater than the window size (shouldn't be here anyway)
					continue
				if listOfIntervals[high].timedelta == d2: #pass intervals that are the same as the previous one
					continue

				d2 = listOfIntervals[high].timedelta #set d2

				if d1==d2: continue #pass if the interval is length zero

				# the probability of an interval of (d1,d2) after an eventA to contain at least one eventB
				intervals = [(interval.timedelta>=d1 and interval.timedelta<=d2,
						  1 - (1 - float(interval.N_b) / float(interval.T))**(d2-d1+1),
						  interval.first_id,
						  interval.last_id,
						  interval.observation_period,
						  interval.observation_period_T)
						  for interval in listOfIntervals]

				probs = [interval[1] for interval in intervals]
				prob = sum(probs) / len(probs)
				N_ab = sum([interval[0] for interval in intervals])

				# the probability of observing at least N_ab intervals with at least one eventB, out of N_a trials
				if not self.poisson_exact:
					p_value = self.calculatePvalue(successes=N_ab, trials=N_a, prob=prob, exact=False)
				else:
					p_value = self.calculatePvalue(successes=N_ab, trials=N_a, prob=probs, exact=True)

				# print intervals
				# print "N_a: {0}, N_ab: {1}, Critical interval: ({2}), P(success): {3:.4f}, p_value: {4:.4f}\n".format(N_a, N_ab, str((d1,d2)), prob, p_value)
				
				if p_value < p_value_threshold:

					
					# check for kl_divergence between those with and those without this event type
					if self.kl_cutoff:
						observation_periods_with_event = [interval[4] for interval in intervals if interval[0]]
						if self.trimWithKL(observation_periods_with_event):
							return None, None

					# check for event count above threshold
					if self.limit_infrequent_events:
						if N_ab < self.limit_infrequent_events:
							return None, None

					# if it passes all tests, add the event type and its instances
					for interval in intervals:
						createNew, p, first_id, last_id, observation_period, T = interval
						#self.addNewEvent(first_id, last_id, candidatePattern, observation_period, T)

					self.addNewCandidatePatterns(candidatePattern, (d1,d2))

					
					print "Distribution for {0}".format(candidatePattern.prettyPrint(self.mapCounterToEventtype))
					print "N_a: {0}, N_ab: {1}, Critical interval: ({2}), P(success): {3:.4f}, p_value: {4:.4f}\n".format(N_a, N_ab, str((d1,d2)), prob, p_value)
					return filter(lambda x: x[0]==True, intervals), (d1, d2)

		return None, None

	def addNewEvent(self, first_id, last_id, event_type, observation_period, observation_period_T):

		self.dbCursor.execute('SELECT time_start FROM {0} WHERE id={1}'.format(self.table, first_id))
		time_start = self.dbCursor.fetchall()[0][0]
		self.dbCursor.execute('SELECT time_end FROM {0} WHERE id={1}'.format(self.table, last_id))
		time_end = self.dbCursor.fetchall()[0][0]

		self.dbCursor.execute('''SELECT happiness FROM {0} WHERE id={1}'''.format(self.table, last_id))
		happiness = self.dbCursor.fetchall()[0][0]

		# insert new row for this occurrence of new event_type
		columns_string1 = "(observation_period, observation_period_T, event_type, time_start, time_end, happiness)"
		columns_string2 = "{0}, {1}, '{2}', {3}, {4}, {5}".format(observation_period, observation_period_T, event_type, time_start, time_end, happiness)


		insert = "INSERT INTO {0} {1} SELECT {2} FROM {0} WHERE id={3}".format(self.table, columns_string1, columns_string2, last_id)
		self.dbCursor.execute(insert)
	
	def pruneThisPair(self, eventType1, eventType2):
		q = ''' SELECT COUNT(*)
							FROM
								(
								SELECT a.id
								FROM
									(
									SELECT id,observation_period
									FROM {0}
									WHERE event_type='{1}'
									) a,
									(
									SELECT id,observation_period
									FROM {0}
									WHERE event_type='{2}'
									) b
								WHERE a.observation_period=b.observation_period
								AND NOT a.id=b.id
								GROUP BY a.observation_period
								) c
					'''.format(self.table, eventType1, eventType2)

		self.dbCursor.execute(q)
		count_with_both = self.dbCursor.fetchall()[0][0]

		# if count_with_both < self.limit_infrequent_pairs:
		# 	print "Pruned candidate pattern (count:{0}) ({1} {2})".format(count_with_both, eventType1.prettyPrint(self.mapCounterToEventtype), eventType2.prettyPrint(self.mapCounterToEventtype))
		# else:
		# 	print "Didn't prune candidate pattern (count:{0}) ({1} {2})".format(count_with_both, eventType1.prettyPrint(self.mapCounterToEventtype), eventType2.prettyPrint(self.mapCounterToEventtype))
		return count_with_both < self.limit_infrequent_pairs

	def unprunedPairs(self):
		q = '''
		 	SELECT COUNT(observation_period) AS count,event_type1,event_type2
			FROM
				(
				SELECT a.event_type as event_type1, b.event_type as event_type2,a.observation_period
				FROM
					(
					SELECT id,observation_period,event_type
					FROM {0}
					) a,
					(
					SELECT id,observation_period,event_type
					FROM {0}
					) b
				WHERE a.observation_period=b.observation_period
				AND NOT a.id=b.id
				GROUP BY a.event_type,b.event_type,a.observation_period
				) c
			GROUP BY event_type1,event_type2
			HAVING count>={1}
			ORDER BY CAST(event_type1 AS UNSIGNED),CAST(event_type2 AS UNSIGNED)
		'''.format(self.table, self.limit_infrequent_pairs)


		self.dbCursor.execute(q)
		return [(e1,e2) for _,e1,e2 in self.dbCursor.fetchall()]
		
	def addNewCandidatePatterns(self, pattern, critical_interval):
		pattern.critical_interval = critical_interval
		self.eventTypes.append(pattern)
		self.t_patterns_found.append(pattern)

		
		for eventType in self.eventTypes:
			prune_first = False
			prune_last = False

			# prune infrequent pairs
			if self.limit_infrequent_pairs:
				if self.pruneThisPair(eventType, pattern):
					prune_first = True
				if self.pruneThisPair(pattern, eventType):
					prune_last = True

			# otherwise add candidate patterns
			if not prune_first:
				newPattern1 = EventType(first_event_type=pattern, last_event_type=eventType)
				self.candidatePatterns.append(newPattern1)
			if not prune_last:
				newPattern2 = EventType(first_event_type=eventType, last_event_type=pattern)
				self.candidatePatterns.append(newPattern2)

	def completenessCompetition(self, eventTypeList, outputdatafile):
		print
		completePatterns = []

		for i in xrange(len(eventTypeList)):
			event_type = eventTypeList[i]

			if event_type.baseEventType:
				continue

			other_events = filter(lambda x: x!=event_type and not x.baseEventType, eventTypeList)

			if any(map(lambda x: event_type in x, other_events)):
				print "Considering completeness of {0}: Not complete".format(event_type.prettyPrint(self.mapCounterToEventtype))
				pass
			else:
				if any(map(lambda x: event_type.getListOfEvents()==x.getListOfEvents(), other_events)):
					iAmLowest = True
					for otherPattern in other_events:
						if event_type.getListOfEvents()==otherPattern.getListOfEvents() and str(event_type)>str(otherPattern):
							print "Considering completeness of {0}: Skipping tie with {1}".format(event_type.prettyPrint(self.mapCounterToEventtype), otherPattern.prettyPrint(self.mapCounterToEventtype))
							iAmLowest = False
							break
					if iAmLowest:
						print "Considering completeness of {0}: Complete".format(event_type.prettyPrint(self.mapCounterToEventtype))
						completePatterns.append(event_type)
				else:
					print "Considering completeness of {0}: Complete".format(event_type.prettyPrint(self.mapCounterToEventtype))
					completePatterns.append(event_type)

		print "\nComplete patterns:"

		output_dataset = [[0 for _ in xrange(len(completePatterns))] for _ in xrange(self.num_observation_periods)]
		for i in xrange(len(completePatterns)):
			completePattern = completePatterns[i]
			ops_with_pattern = self.getObservationPeriodsWithEventType(completePattern)
			for op in ops_with_pattern:
				output_dataset[op-1][i] = 1
			print "Pattern {0}: {1}".format(i, completePattern.prettyPrint(self.mapCounterToEventtype))
			#print "Observation Periods with this pattern: {0}\n".format(ops_with_pattern)

		
		with open(outputdatafile, 'w+') as f:
			w = csv.writer(f, delimiter=',')
			w.writerows(output_dataset)
		print "Output dataset written to {0}:".format(outputdatafile)


	def calculatePvalue(self, successes, trials, prob, exact=False):
		if not exact:
			p_value = 1 - binom.cdf(max(0,successes-1), trials, prob)

		else:
			p_value = 0
			for num_of_successes in xrange(successes, len(prob)+1):
				flags = [1]*num_of_successes + [0]*(len(prob)-num_of_successes)
				perms_seen = set()
				for flag_perm in itertools.permutations(flags):

					if flag_perm in perms_seen:
						continue
					perms_seen.add(flag_perm)
					prob_of_this_state = 1
					for flag,p in zip(flag_perm, prob):
						if flag==1:
							prob_of_this_state *= p
						else:
							prob_of_this_state *= (1-p)
					p_value +=prob_of_this_state

		return p_value

	def trimWithKL(self, observation_periods_with_event):
		observation_periods_with_event = [str(e) for e in observation_periods_with_event]
		happiness_with = '''SELECT happiness FROM {0} WHERE observation_period in ({1}) GROUP BY observation_period'''.format(self.table, ",".join(observation_periods_with_event))
		happiness_without = '''SELECT happiness FROM {0} WHERE observation_period not in ({1}) GROUP BY observation_period'''.format(self.table, ",".join(observation_periods_with_event))

		self.dbCursor.execute(happiness_with)
		dist1 = self.dbCursor.fetchall()
		dist1 = self.classCountsToProbs(dist1)



		self.dbCursor.execute(happiness_without)
		dist2 = self.dbCursor.fetchall()
		dist2 = self.classCountsToProbs(dist2)

		if self.kl_divergence(dist1, dist2) < self.kl_cutoff:
			print "Trimmed event_type with KL divergence: {0}".format(self.kl_divergence(dist1, dist2))
			return True
		else:
			return False

	def classCountsToProbs(self, dist):
		class_counts = {'1':0, '2':0, '3':0, '4':0, '5':0}
		total = 0
		for row in dist:
			total +=1
			class_value = str(row[0])
			class_counts[class_value] +=1

		dist = [class_counts[k]/float(total) for k in sorted(class_counts)]

		return dist

	def kl_divergence(self, dist1, dist2):
		epsilon = 10e-6
		kl = 0
		dist1 = [ (x+epsilon)/(1+epsilon*len(dist1)) for x in dist1]
		dist2 = [ (x+epsilon)/(1+epsilon*len(dist2)) for x in dist2]

		for i in xrange(len(dist1)):
			Y_i = dist1[i]
			X_i = dist2[i]
			kl += math.log( Y_i/X_i ) * Y_i

		return kl

	def getObservationPeriodsWithEventType(self, eventType):
		q = ''' SELECT DISTINCT observation_period
				FROM {0}
				WHERE event_type='{1}'
				ORDER BY observation_period
				'''.format(self.table, eventType)

		self.dbCursor.execute(q)

		return [int(_[0]) for _ in self.dbCursor.fetchall()]


	# def trimInfrequentEvents(self, iterator, threshold):
		# for event_type in self.eventTypes:
		# 	self.eventTypeCounts[event_type] = 0

		# for observationPeriod in iterator:
		# 	for event in observationPeriod:

		# 		self.eventTypeCounts[event.event_type] +=1

		# for k in self.eventTypeCounts:
		# 	if self.eventTypeCounts[k] < threshold:
		# 		self.eventTypes = filter(lambda x: x!=k, self.eventTypes)

	# def processObservationPeriod(self, observationPeriod, candidatePattern):
		# """For one observation period, add N_b,T,interval for all event-type pairs to the appropriate distribution 
		# in the TPattern object. For each eventA, the next instance of each event type is considered an interval.
		# For each eventB, it is only considered an interval for the closest instance of each eventA."""
		# totalTime = observationPeriod.events[-1].last_timestamp - observationPeriod.events[0].first_timestamp
		# totalTime = int(totalTime.total_seconds()/(60*60))

		# if not (candidatePattern.first_event_type.baseEventType and candidatePattern.last_event_type.baseEventType):
		# 	observationPeriod.addEventsForSubpatterns(candidatePattern)


		# observationPeriod.events = filter(lambda x: x.event_type in [candidatePattern.first_event_type, candidatePattern.last_event_type], observationPeriod.events)

		# # for e in observationPeriod.events:
		# # 	print e, e.first_timestamp
		# # print

		# intervals = []
		# countA = 0
		# countB = 0
		# # iterate over each event instance in reverse, to make sure you only consider the closest instance of eventA to eventB
		# laterEventInstancesUsed = set()
		# for i in xrange(len(observationPeriod)-1, -1, -1):


		# 	event_instance1 = observationPeriod[i]
		# 	if event_instance1.event_type == candidatePattern.first_event_type:
		# 		countA+=1
		# 	# if event_instance1.event_type == candidatePattern.last_event_type:
		# 	# 	countB+=1
		# 	if event_instance1.event_type != candidatePattern.first_event_type:
		# 		continue

		# 	# iterate over each later event instance
		# 	found_one = False
		# 	for event_instance2 in observationPeriod[i+1:]:
		# 		if not event_instance2.event_type == candidatePattern.last_event_type:
		# 			continue
		# 		if event_instance1.last_timestamp >= event_instance2.first_timestamp:
		# 			continue
				
		# 		# get timedelta in between events in hours
		# 		timedelta = event_instance2.first_timestamp - event_instance1.last_timestamp
		# 		timedelta = int(timedelta.total_seconds()/(60*60))+1
		# 		if timedelta > 24:
		# 			break

		# 		if not found_one and not event_instance2 in laterEventInstancesUsed:
					

		# 			laterEventInstancesUsed.add(event_instance2)
		# 			found_one = True
					
		# 			#interval = Interval(timedelta, event_instance1.id, event_instance2.id, eventTypeCounts[event_instance2.event_type], totalTime)

		# 			bisect.insort(intervals, timedelta)

		# 		countB+=1



		# #intervals = map(lambda x: Interval(x, countB, totalTime), intervals)
		# intervals = map(lambda x: Interval(x, countB, countA*24), intervals)

		# return countA, intervals

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

	# def addEvent(self, event):

		# bisect.insort(self.events, event)

	# def addEventsForSubpatterns(self, pattern):
		# events_to_add = []

		# if not pattern.first_event_type.baseEventType:
		# 	self.addEventsForSubpatterns(pattern.first_event_type)

		# if not pattern.last_event_type.baseEventType:
		# 	self.addEventsForSubpatterns(pattern.last_event_type)

		# if not pattern.critical_interval: return None

		# for i in xrange(len(self.events)):
			
		# 	event1 = self.events[i]
		# 	if not event1.event_type == pattern.first_event_type: continue

		# 	for j in xrange(i+1, len(self.events)):
		# 		event2 = self.events[j]
		# 		if not event2.event_type == pattern.last_event_type: continue

				
		# 		td = (event2.first_timestamp - event1.last_timestamp)
		# 		td = int(td.total_seconds() / (60.*60.))

		# 		if td > pattern.critical_interval[0] and td < pattern.critical_interval[1]:
		# 			e = EventInstance(event_type=pattern, eventID=None, first_timestamp=event1.first_timestamp, last_timestamp=event2.last_timestamp)
		# 			events_to_add.append(e)
		# 			break

		# for e in events_to_add:
		# 	bisect.insort(self.events, e)

	def getIntervals(self,candidatePattern):
		first_event = candidatePattern.first_event_type
		last_event = candidatePattern.last_event_type
		print first_event
		print last_event
		for event in self.events:
			eventid,claimnumber,ts,event_type = event
			event_type = self.mapEventTypeToCounter[event_type]
			if event_type==first_event_type:
				print "first_event_type: {0}".format(event_type)


class Interval(object):
	def __init__(self, timedelta, N_b=None, T=None, first_id=None, last_id=None, observation_period=None, observation_period_T=None):
		self.timedelta = timedelta
		self.N_b = N_b
		self.T = T
		self.first_id = first_id
		self.last_id = last_id
		self.observation_period = observation_period
		self.observation_period_T = observation_period_T

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
			#self.name = "xxxxo{0}_{1}xxxxc".format(self.first_event_type, self.last_event_type)
			self.name = "o{0}_{1}c".format(self.first_event_type, self.last_event_type)
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

	def prettyPrint(self, mapCounterToEventtype):
		#print "self", self, self.baseEventType
		if not self.baseEventType:
			if self.critical_interval:
				#print "here1"
				return "({0} {1},{2} {3})".format(self.first_event_type.prettyPrint(mapCounterToEventtype), self.critical_interval[0], self.critical_interval[1], self.last_event_type.prettyPrint(mapCounterToEventtype))
			else:
				#print "here2"
				return "({0} {1})".format(self.first_event_type.prettyPrint(mapCounterToEventtype), self.last_event_type.prettyPrint(mapCounterToEventtype))
		else:
			#print "here3"
			return "{0}".format(mapCounterToEventtype[self.name])
