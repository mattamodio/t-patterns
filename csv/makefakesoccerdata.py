import random

outfile = 'fakesoccerdata.csv'
outfile_classification = 'fakesoccerdata_classification.csv'



num_observation_periods = 100


with open(outfile, 'w+') as f:
	with open(outfile_classification, 'w+') as g:
		counter=0

		for i in xrange(1,num_observation_periods+1):
			events = [[i, 0, 0, 'start'], [i, 45, 45, 'halftime'], [i, 90, 90, 'end']]
			score = []

			########################################
			# simulate game
			for team in ['home','away']:
				# goals
				num_goals = random.randint(0,4)
				score.append(num_goals)

				for _ in xrange(num_goals):
					time = random.randint(0,90)
					event = '{}_goal'.format(team)
					events.append( [i,time,time,event] )


				# corners
				num_corners = random.randint(0,0)

				for _ in xrange(num_corners):
					time = random.randint(0,90)
					event = '{}_corner'.format(team)
					events.append( [i,time,time,event] )



			########################################
			# write data out
			events = sorted(events)

			for index in xrange(len(events)):
				events[index] = [counter] + events[index]
				counter+=1

			events = ",".join(map(lambda x: "({},{},{},{},'{}')".format(x[0],x[1],x[2],x[3],x[4]), events))

			f.write('[')
			f.write(events)
			f.write(']\n')

			########################################
			# write classification out

			if score[1]>score[0]: classification=1.
			elif score[1]==score[0]: classification=.5
			else: classification=0.
			classification = "[{},{}]\n".format(i,classification)

			g.write(classification)

