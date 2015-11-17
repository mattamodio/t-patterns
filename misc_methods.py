SHELVE_FILE = '/Users/matthewamodio/Documents/School Code/research/AmFam/TimeLineFeatures/Events/claims170k_03112015_new.shelve'


def formSQLinsert(dictToInsert, tableToInsertInto):
    columns = '('
    values = '('
    for k,v in dictToInsert.iteritems():
        columns += "`" + k + "`" + ","
        values += "'" + cleanseValue(v) + "'" + ","
    columns = columns[:-1] + ')'
    values = values[:-1] + ')'

    return "INSERT IGNORE INTO {0} {1} \
            VALUES {2} \
            ".format(tableToInsertInto, columns, values)

def addToTable(data, table, dbCursor):
    '''
    Adds the data in dictionary form to the table, provided the keys are already columns in the table.
    '''

    dbCursor.execute(  formSQLinsert(data, table)  )

def cleanseValue(cellString):
    cellString = cellString.replace("'", "''")
    return cellString

def shelveToSQL():
	d = shelve.open(SHELVE_FILE)

	#################################################################
	# go through each row, printing updates every 5k out of the 170k
	for i in xrange(1,ROWS):


		if i % 5000 == 0:
			print "Row {0} at {1}".format(i, datetime.datetime.now())
		
		row = d[str(i)]

		for event in row:
			if event[1] == 'PC': colList = PC_list
			elif event[1] == 'AL': colList = AL_list
			elif event[1] == 'NT': colList = NT_list

			data = {}
			def assign(zipped):
				col, value = zipped
				data[col] = value
			map(assign, zip(colList, event))

			addToTable(data, TABLE, dbCursor)
	#################################################################

	# clean up
	d.close()
