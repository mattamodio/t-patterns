import bisect
from scipy.stats import binom

class TPattern(object):
	def __init__(self):
		self.eventTypeMap = {}
		self.dbColumns = []
		self.eventDistributions = {}
		self.tpatterns = []


	def setDB(self, dbName, table, dbCursor, eventTypeColumn):
		self.dbName = dbName
		self.tableName = table
		self.dbCursor = dbCursor
		self.eventTypeColumn = eventTypeColumn
		self.getColumnNames()
		self.getEventTypeMap()

	def getColumnNames(self):
		query = "SELECT * \
				 FROM `INFORMATION_SCHEMA`.`COLUMNS` \
				 WHERE `TABLE_SCHEMA`='{0}' AND `TABLE_NAME`='{1}';".format(self.dbName, self.tableName)

		self.dbCursor.execute(query)

		self.dbColumns = [result[3] for result in self.dbCursor.fetchall()]

	def getEventTypeMap(self):
		query = "SELECT DISTINCT {0} \
				 FROM {1} \
				 ORDER BY {0} ASC".format(self.eventTypeColumn, self.tableName)

		self.dbCursor.execute(query)

		i = 0
		for eventtype in self.dbCursor.fetchall():
			self.eventTypeMap[i] = eventtype[0]
			i +=1


	def timeToNextEvent(self, eventType):
		query = """
				SELECT a.id, a.eventtype as fromevent, b.eventtype as toevent, TIMESTAMPDIFF(hour, a.ts, b.ts) as timediff
				FROM 
					(SELECT id, claimnumber, ts, eventtype
					FROM copy
					WHERE eventtype="{0}"
					ORDER BY id) a,
					(SELECT id, claimnumber, ts, eventtype
					FROM copy
					ORDER BY id) b
				WHERE a.claimnumber=b.claimnumber AND a.id < b.id

				#GROUP BY a.id, b.eventtypettype
				ORDER BY fromevent, toevent, a.id
				""".format(eventType)



		self.dbCursor.execute(query)

		# initialize with first event:
		eventId, fromevent, toevent, timediff = self.dbCursor.fetchone()
		previousToEvent = toevent
		idsInDistribution = set([eventId])
		distributionFirstSeen = [timediff]
		distributionLaterSeen = []
		counter =1
		

		for event in self.dbCursor.fetchall():
			eventId, fromevent, toevent, timediff = event

			# if this is the same distribution we've been working on and if we've seen this id before
			if previousToEvent == toevent and eventId not in idsInDistribution:
				bisect.insort(distributionFirstSeen, timediff)
				idsInDistribution.add(eventId)

			elif previousToEvent == toevent:
				distributionLaterSeen.append(timediff)

			else:
				print "Looking for critical intervals for {0}-{1}...".format(eventType, previousToEvent)
				criticalInterval = lookForCriticalIntervals(distributionFirstSeen, distributionLaterSeen)
				if criticalInterval:
					self.tpatterns.append((fromevent, toevent, criticalInterval))
				print "Finished!\n"

				# clear the collectors for next event type
				previousToEvent = toevent
				distributionFirstSeen = [timediff]
				idsInDistribution = set([eventId])
				distributionLaterSeen = []

			counter+=1
			if counter > 5000:
				print self.tpatterns
				break


def lookForCriticalIntervals(distributionFirstSeen, distributionLaterSeen):
	""" Searches for critical intervals by considering intervals from distributionFirstSeen, and then calculates p-value using
	both distributions according to t-pattern algorithm."""

	for low in xrange(len(distributionFirstSeen)):
		for high in xrange(len(distributionFirstSeen)-1, low, -1):

			d1 = distributionFirstSeen[low]
			d2 = distributionFirstSeen[high]

			if d1 == d2:
				continue

			N_a = 47
			N_b = 1
			T = 8549
			N_ab = sum( 
					map(lambda pair: all(pair), 
						zip(map(lambda x: d1<=x, distributionFirstSeen), 
							map(lambda x: x<=d2, distributionFirstSeen)))) \
					+ sum(
					map(lambda pair: all(pair),
						zip(map(lambda x: d1<=x, distributionLaterSeen),
							map(lambda x: x<=d2, distributionLaterSeen))))

			# the probability of any random hour including a B event type, given uniform spread over time
			prob = 1 - (1 - float(N_b) / T)**(d2-d1+1)

			p_value = 1 - binom.cdf(N_ab, T, prob)

			if p_value < .05:
				#print distributionFirstSeen
				#print distributionLaterSeen
				print "d1: {0}, d2: {1}, p_value: {2}".format(d1, d2, p_value)
				return (d1, d2)

	return None








