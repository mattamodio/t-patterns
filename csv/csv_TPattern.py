import bisect
import sys
import math
import ast
import itertools
import csv
from scipy.stats import binom, norm


class TPattern(object):
	def __init__(self, datafile=None, datafile_classification=None, limit_infrequent_events=False, limit_infrequent_pairs=False, window=False, original_table=None, kl_cutoff=None, poisson_exact=False):
		self.eventTypes = []
		self.t_patterns_found = []
		self.eventTypeCounts = {}
		self.candidatePatterns = []
		self.limit_infrequent_events = limit_infrequent_events
		self.kl_cutoff = kl_cutoff
		self.poisson_exact = poisson_exact
		self.limit_infrequent_pairs = limit_infrequent_pairs
		self.datafile = datafile
		self.datafile_classification = datafile_classification
		self.num_events = 0

		self.observation_periods = []

		if not window:
			self.window=sys.maxint
		else:
			self.window = window
		
	def getEventTypesAndOPs(self):
		# create event types
		event_types_found = {}
		with open(self.datafile, 'rb') as f:
			num = 0
			for row in f:
				row = ast.literal_eval(row)

				op = ObservationPeriod(events=row)
				self.observation_periods.append(op)

				for event in row:
					self.num_events+=1

					eventid,claimnumber,ts_begin,ts_end,event_type_name = event

					if event_type_name not in event_types_found:
						event_types_found[event_type_name] = 1

						event_type = EventType(name=event_type_name)
						self.eventTypes.append(event_type)
					else:
						event_types_found[event_type_name] +=1

			if self.limit_infrequent_events:
				self.eventTypes = []
				frequent_events = map(lambda x: x if event_types_found[x]>=self.limit_infrequent_events else None, event_types_found)
				for frequent_event in filter(lambda x: x, frequent_events):
					event_type = EventType(name=frequent_event)
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

					candidatePattern.setCriticalInterval( (d1,d2) )
					# if it passes all tests, add the event type and its instances
					for interval in intervals:
						createNew, p, first_id, last_id, observation_period, T = interval

						self.addNewEvent(first_id, last_id, candidatePattern, observation_period)


					self.addNewCandidatePatterns(candidatePattern)

					
					print "Distribution for {0}".format(candidatePattern)
					print "N_a: {0}, N_ab: {1}, Critical interval: ({2}), P(success): {3:.4f}, p_value: {4:.4f}\n".format(N_a, N_ab, str((d1,d2)), prob, p_value)
					return filter(lambda x: x[0]==True, intervals), (d1, d2)

		return None, None

	def addNewEvent(self, first_id, last_id, new_event_type, observation_period):

		for event in self.observation_periods[observation_period-1]:
			eventid,claimnumber,ts_begin,ts_end,event_type = event
			if eventid==first_id:
				new_ts_begin = ts_begin
			if eventid==last_id:
				new_ts_end = ts_end

		self.num_events+=1


		newevent = (self.num_events,observation_period,new_ts_begin,new_ts_end,new_event_type)

		self.observation_periods[observation_period-1].events.append(newevent)

		self.observation_periods[observation_period-1].events = sorted(self.observation_periods[observation_period-1].events,
														key=lambda x: x[2])
	
	def pruneThisPair(self, eventType1, eventType2):
		count_with_both = 0

		for op in self.observation_periods:
			has_e1,has_e2 = [False]*2
			for event in op:
				eventid,op_num,ts_begin,ts_end,event_type = event

				if event_type==eventType1:
					has_e1 = True
				elif event_type==eventType2:
					has_e2 = True

			if has_e1 and has_e2: count_with_both+=1

		# if count_with_both < self.limit_infrequent_pairs:
		# 	print "Pruned candidate pattern (count:{0}) ({1} {2})".format(count_with_both, eventType1, eventType2)
		# else:
		# 	print "Didn't prune candidate pattern (count:{0}) ({1} {2})".format(count_with_both, eventType1, eventType2)
		return count_with_both < self.limit_infrequent_pairs
	
	def addNewCandidatePatterns(self, pattern):
		
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

		output_dataset = [[0 for _ in xrange(len(completePatterns))] for _ in xrange(len(self.observation_periods))]
		for i in xrange(len(completePatterns)):
			completePattern = completePatterns[i]
			ops_with_pattern = self.getObservationPeriodsWithEventType(completePattern)
			for op in ops_with_pattern:
				output_dataset[op-1][i] = 1
			print "Pattern {0}: {1}".format(i, completePattern.prettyPrint())
			print "Observation Periods with this pattern: {0}\n".format(ops_with_pattern)

		
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

		dist1 = []
		dist2 = []

		with open(self.datafile_classification) as f:
			for line in f:
				line = ast.literal_eval(line)
				observation_period, classification = line
				if str(observation_period) in observation_periods_with_event:
					dist1.append(classification)
				else:
					dist2.append(classification)

		if not (dist1 and dist2): return False

		unique_values = set()
		for value in dist1+dist2:
			unique_values.add(value)
		countDict = {v:0 for v in unique_values}

		dist1 = self.classCountsToProbs(dist1, countDict.copy())
		dist2 = self.classCountsToProbs(dist2, countDict.copy())

		if self.kl_divergence(dist1, dist2) < self.kl_cutoff:
			print "Trimmed event_type with KL divergence: {0}".format(self.kl_divergence(dist1, dist2))
			return True
		else:
			return False

	def classCountsToProbs(self, dist, countDict):
		total = 0
		for value in dist:
			total +=1
			countDict[value] +=1

		dist = [countDict[k]/float(total) for k in sorted(countDict)]

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
		ops_with = []
		for op in self.observation_periods:
			for event in op:
				eventid,op_num,ts_begin,ts_end,event_type = event
				if event_type==eventType:
					ops_with.append(op_num)
					break


		return ops_with


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

class EventType(object):
	def __init__(self, name=None, first_event_type=None, last_event_type=None, critical_interval=None):
		self.first_event_type = first_event_type
		self.last_event_type = last_event_type
		self.critical_interval = critical_interval
		if not name:
			#self.name = tpattern.mapCounterToEventtype
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
		if isinstance(other, str):
			return self.name==other

		if isinstance(other, EventType):
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

	def setCriticalInterval(self, critical_interval):
		self.critical_interval = critical_interval
		self.name = "({} {},{} {})".format(self.first_event_type, critical_interval[0], critical_interval[1], self.last_event_type)

	def prettyPrint(self):
		return str(self).replace('(','').replace(')','')




