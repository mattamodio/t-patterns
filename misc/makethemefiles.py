typefile = 'event_types.txt'
out = '/Users/matthewamodio/Desktop/vvt.vvt'
import ast, sys, datetime, string

with open(typefile) as f:
	with open(out, 'w+') as g:
		types = ast.literal_eval(f.read())
		for i in xrange(len(types)):
			types[i] = types[i].replace(' ', '').replace('/', '')
		for t in types:
			g.write("{0}\n {0}\n\n".format(t))


personfile = '/Users/matthewamodio/Desktop/claim_events_personrows.csv'

with open(personfile) as f:

	i=0

	for person in f:
		i+=1
		person = ast.literal_eval(person)

		with open('/Users/matthewamodio/Desktop/personfiles/p{0}.txt'.format(i), 'w+') as g:

			g.write('Time\tEvent\n')

			first = True

			for event_instance in person:

				event_id, claimnumber, timestamp, event_type = event_instance
				event_type = event_type.replace(' ', '').replace('/', '')
				timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

				hour = int(int(timestamp.strftime("%s")) / (60. * 60.))

				if first:
					g.write('{0}\t:\n'.format(hour))
					first = False

				g.write('{0}\t{1}\n'.format(hour, event_type))


			g.write('{0}\t&\n'.format(hour))


		if i>10000:
			sys.exit()