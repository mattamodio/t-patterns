import sys
import shelve
import datetime
import MySQLdb
import matplotlib.pyplot as plt
import TPattern
import ast
import bisect
import csv


def connect(db):
	conn = MySQLdb.connect( host='127.0.0.1', user='root', db=db, charset='utf8')
	conn.autocommit(True)

	cursor = conn.cursor()

	return cursor

def addColumns(cursor, table, event_type):
	q1 = '''ALTER TABLE {0} ADD interval_{1} INT'''.format(table, event_type)
	q2 = '''ALTER TABLE {0} ADD interval_{1}_id INT'''.format(table, event_type)
	# print q1
	# print q2
	cursor.execute(q1)
	cursor.execute(q2)

def createIntervals(db, table, event_types):
	cursor = connect(db)
	
	for event_type in event_types:
		addColumns(cursor, table, event_type)

	q = '''
	SELECT a.id, b.id, a.event_type, b.event_type, b.time_start-a.time_end as timediff
	FROM {0} a, {0} b
	WHERE a.observation_period = b.observation_period
	AND NOT a.id = b.id
	AND b.time_start-a.time_end>0
	GROUP BY a.id, b.event_type
	'''.format(table)

	cursor.execute(q)
	for row in cursor.fetchall():
		id1, id2, event_type1, event_type2, timediff = row
		q = '''UPDATE {0} SET interval_{1}='{2}', interval_{1}_id='{3}' WHERE id={4}'''.format(table, event_type2, timediff, id2, id1)
		# print q
		cursor.execute(q)

def main():
	pass
	#createIntervals(db, table, event_types)


if __name__=="__main__":
	main()