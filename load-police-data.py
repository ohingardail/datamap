#! /usr/bin/python
# Script to load police data (https://data.police.uk/)

# load required libraries
import getopt, sys, pprint, copy, re, json, pdb
from time import sleep
from types import *
from datetime import datetime, timedelta
import calendar
from dateutil import relativedelta
import requests 		# http://docs.python-requests.org/en/master/
import mysql.connector 		# https://dev.mysql.com/doc/connector-python/en

# experimental codec failure override
# otherwise mysql_cursor.execute(query) returns occasional
# An exception of type UnicodeDecodeError occurred. Arguments:
# ('utf8', '\x00\xfb', 1, 2, 'invalid start byte')
import codecs
codecs.register_error("strict", codecs.ignore_errors) # this is a BAD SOLUTION

# hard coded defaults
region = 'Reading Borough'
user = 'datamap' 	# mysql user
password = 'datamap' 	# mysql password
host = 'localhost' 	# mysql host
database = 'datamap'	# mysql database
api_url = 'https://data.police.uk/api/'
options = ''		# specialised options for swifter loading, debugging etc

# police API wait and retry parms
wait = 10 	# seconds
retry = 6 	# times

# always returns YYYY-MM-DD where DD is the last day of the month and MM is 01-12
def standardise_date(date_string):
	if date_string is None or len(date_string) == 0:
		return None
	# convert to date
	try:
		date_value = datetime.strptime(date_string, '%Y-%m-%d')
	except:
		print "ERR: unable to convert string '" + date_string + "' into a date"
		return None
	date_string = str(date_value.year) + '-' + str(date_value.month).rjust(2, '0') + '-' + str(calendar.monthrange(date_value.year, date_value.month)[1])
	return date_string

# calls specified mysql function and returns result
def mysql_function(function, params):
	if function is None:
		print "ERR: mysql_function : must specify function"
		return None
	query = 'select '+ function + '('
	if len(params) == 0:
		query = query + ')'
	else:
		for param in params:
			#print 'DBG: param=' + repr(param)
			if param is None:
				query = query + "null,"
			elif type(param) is StringType or type(param) is UnicodeType:
				#print 'DBG: param is Unicode'
				try:
					param = str(param.encode('utf-8', 'ignore'))
				except Exception as ex:
					print 'ERR: mysql_function param utf-8 conversion failed: "' + str(param) + '".'
  					template = " - An exception of type {0} occurred. Arguments:\n{1!r}"
    					message = template.format(type(ex).__name__, ex.args)
    					print message
					sys.exit(1)
				param = re.sub('[\"\']', '', param) # delete quotes
				query = query + "'" + param + "',"
			else:
				#print 'DBG: param is not Unicode'
				query = query + str(param) + ","
		query = re.sub(',$', ')', query)
	#print "DBG: X"
	mysql_cursor = db.cursor()
	#print "DBG: Y"
	try:
		#print "DBG: query = " + query
		mysql_cursor.execute(query)
		#print "DBG: Z"
	#except mysql.connector.Error as err:
	#	print 'ERR: mysql_function query "' + query + '" failed.'
	#	print("ERR: mysql connect error: {}".format(err))
	#	return None
	#except:
	#	print 'ERR: mysql_function query "' + query + '" failed.'
	#	print 'ERR: cursor warnings: ' + str(mysql_function.fetchwarnings())
	#	return None
	except Exception as ex:
		print 'ERR: mysql_function query ' + repr(query) + ' failed.'
  		template = " - An exception of type {0} occurred. Arguments:\n{1!r}"
    		message = template.format(type(ex).__name__, ex.args)
    		print message
		#pdb.post_mortem()
		#sys.exit(1)
		#return None
	resultset = mysql_cursor.fetchone()
	#print "DBG: resultset = " + str(resultset)
	#print "DBG: db.commit()..."
	db.commit()
	# print "DBG: DONE db.commit()"
	mysql_cursor.close()
	if resultset is not None:
		return resultset[0]
	return None

# calls specified mysql procedure
def mysql_procedure(procedure, params):
	if procedure is None:
		print "ERR: mysql_procedure : must specify procedure"
		return False
	mysql_cursor = db.cursor()
	try:
		out = mysql_cursor.callproc(procedure, params)
	except mysql.connector.Error as err:
		print("ERR: mysql connect error: {}".format(err))
		return False
	except:
		print 'ERR: cursor warning: ' + str(mysql_cursor.fetchwarnings())
		return False
	db.commit()
	mysql_cursor.close()
	return True

# returns python map (ie assoc array) of JSON returned by given URL
# automatically prepends API base URL
# police data API may return list (of JSON dict) or naked JSON dict
# needs to wait and retry on failure (police website a bit odd)
def get_police_data(url, payload):
	if url is None :
		print "ERR: get_police_data : must specify url"
		return None
	success = False
	attempt = 1
	## loop attempts
	while (attempt <= retry) and not success:
		success = True
		# attempt to get url
		if payload is None:
			try:
				r = requests.get(api_url + url)
			except:
				success = False
		else:
			try:
				r = requests.get(api_url + url, params=payload)
			except:
				success = False
		# expect 200 code back
		if success and (r.status_code != 200):
			success = False
		# prepare to loop on failure
		if not success:
			attempt = attempt + 1
			if attempt <= retry:
				sleep(wait)
	# print "DBG: " + str(r.url)
	# after success or all retries exhausted
	# make sure list of dict always returned
	if success and r.json() is not None:
		if type(r.json()) is ListType:
			return r.json()
		else:
			return [r.json()]
			#print('Unable to get JSON from "' + api_url + url + '"' )
			# sys.exit(1)
			#return None
	else:
		print('ERR: Unable to get data from "' + api_url + url + '" using payload "'+ str(payload) + '".' )
		# sys.exit(1)
		return None

# loads police API contacts or engagement methods into extension blob
# note - data must point to ONE LINE in data array, ie data[1] or data[0] etc
def load_contacts(id, data):
	if 'engagement_methods' in data :
		for engagement_method in data['engagement_methods'] :
			mysql_function('post_extension',[id, engagement_method['title'], engagement_method['url'] ])
	if 'contact_details' in data :
		for field in data['contact_details'] :
			mysql_function('post_extension',[id, field, data['contact_details'].get(field) ])

# loads police force data for the region 
# this is a compromise; actually loads force data for each of the MBR corners of the region. May not work if region is v large
def load_force(place):
	if region is None:
		print "ERR: load_force : must specify place"
		return None
	load_force_cursor = db.cursor()

	# get array of mbr corners
	query = "select convert_geometry_to_police_string(mbr_polygon )from place where name = '" + place + "'"
	try:
		load_force_cursor.execute(query)
	except:
		print('ERR: Unable to get run query "' + query + '"')
		# sys.exit(1)
		return None
	resultset = load_force_cursor.fetchone()
	point_list = resultset[0].split(':')
	del point_list[-1] # last element is same as first
	# get centre point
	query = "select convert_geometry_to_police_string(centre_point) from place where name = '" + place + "'"
	try:
		load_force_cursor.execute(query)
	except:
		print('ERR: Unable to get run query "' + query + '"')
		# sys.exit(1)
		return None
	resultset = load_force_cursor.fetchone()
	point_list.append(resultset[0])

	# get SRID value used in calculating police neighbourhood (not sure if its actually used by mariadb GIS functions)
	SRID = mysql_function('get_constant', ['SRID'])

	# check police force at each point
	force_data = None
	for point in point_list :
		force_id = get_police_data('locate-neighbourhood',{'q': point})[0]['force']

		# load force data
		organisation_id = mysql_function('get_organisation', ['identifier', force_id ])
		if len(json.loads(organisation_id)) > 0 :
			organisation_id = json.loads(organisation_id)[0]['id']
			force_new = False
		else :
			#print "DBG: Loading force : " + force_id
			force_data = get_police_data('forces/'+ force_id, None)
			organisation_id = mysql_function('post_organisation',['police-force', force_id, force_data[0]['name'], force_data[0]['description'] ])
			force_new = True

			if organisation_id is not None :
				mysql_function('post_extension',[organisation_id, 'url', force_data[0]['url'] ])
				mysql_function('post_extension',[organisation_id, 'telephone', force_data[0]['telephone'] ])
				load_contacts(organisation_id, force_data[0])

		# load force neighbourhood data
		count = 0
		neighbourhoods = get_police_data( force_id + '/neighbourhoods', None)
		for neighbourhood in neighbourhoods :
			count = count + 1
			if force_new or mysql_function('exists_place', ['identifier', neighbourhood['id'] ]) == 0 :
				#print "DBG: Loading neighbourhood " + str(count) + "/" + str(len(neighbourhoods)) + " : " + neighbourhood['id']
				specific_neighbourhood = get_police_data( force_id + '/' + neighbourhood['id'], None)
				neighbourhood_id = mysql_function('post_place',['police-neighbourhood', neighbourhood['id'], specific_neighbourhood[0]['name'], specific_neighbourhood[0]['description'], None, None, specific_neighbourhood[0]['centre']['longitude'], specific_neighbourhood[0]['centre']['latitude'], None ])

				if neighbourhood_id is not None:
					mysql_function('post_relation', [None, organisation_id, neighbourhood_id])
					mysql_function('post_extension', [neighbourhood_id, 'population', specific_neighbourhood[0]['population'] ])
					load_contacts(neighbourhood_id, specific_neighbourhood[0])

					# load polygon
					boundaries = get_police_data( force_id + '/' + neighbourhood['id'] + '/boundary', None)
					boundary_string = ''
					for boundary in boundaries:
						if len(boundary_string) == 0:
							boundary_string = boundary['longitude'] + ' ' + boundary['latitude']
							starting_point = boundary_string
						else:
							boundary_string = boundary_string + ',' + boundary['longitude'] + ' ' + boundary['latitude']
					boundary_string = boundary_string + ',' + starting_point
					query = "update place set polygon = ST_GeometryFromText('POLYGON((" + boundary_string  + "))'," + SRID + ") where type = 'police-neighbourhood' and identifier = '" + str(neighbourhood['id'] +"'")
					try:
						load_force_cursor.execute(query)
					except:
						print('ERR: Unable to get run query "' + query + '"')
						return False
	load_force_cursor.close()
	if force_data is not None:
		return len(force_data)
	else:
		return 0

# loads police categories
def load_categories(month):
	# check if month is valid
	month = standardise_date(month)
	if datetime.strptime(month, '%Y-%m-%d') > datetime.today():
		print "ERR: load_crime : specify past month (" + month + ")"
		return None
	month_string = '-'.join((month.split('-')[0], month.split('-')[1]))

	load_category_cursor = db.cursor()

	# load crime categories
	crime_categories = get_police_data('crime-categories',{'date': month_string})
	if crime_categories is not None and len(crime_categories) > 0:
		for crime_category in crime_categories:
			query = "select count(*) from category where type = 'police-crime' and identifier = '" + re.sub('[ -]+', '-', crime_category['url'].strip()) + "'"
			try:
				load_category_cursor.execute(query)
			except:
				print('ERR: Unable to get run query "' + query + '"')
				# sys.exit(1)
				return 0
			category_count = load_category_cursor.fetchone()[0]
			# print "DBG: category_count = " + str(category_count)
			if category_count == 0 :
				output = mysql_function('post_category', [
								'police-crime', 
								re.sub('[ -]+', '-', crime_category['url'].strip()), 
								crime_category['name'], 
								'https://www.police.uk/about-this-site/faqs/#what-do-the-crime-categories-mean'
								])
				# print "DBG: output = " + str(output)
	load_category_cursor.close()
	if crime_categories is not None:
		return len(crime_categories)
	else:
		return 0

# loads crime data
def load_crimes(place, month):
	# check if month is valid
	month = standardise_date(month)

	if region is None or month is None or datetime.strptime(month, '%Y-%m-%d') > datetime.today():
		print "ERR: load_crime : must specify both region and month (YYYY-MM-DD) and month must be in the past."
		return 0

	# convert geography params into forms used by police API
	load_crime_cursor = db.cursor()
	query = "select convert_geometry_to_police_string(mbr_polygon )from place where name = '" + place + "'"
	try:
		load_crime_cursor.execute(query)
	except:
		print('ERR: Unable to get run query "' + query + '"')
		# sys.exit(1)
		return 0
	place_string = load_crime_cursor.fetchone()
	load_crime_cursor.close()
	month_string = '-'.join((month.split('-')[0], month.split('-')[1]))

	# load crime data (returns no data before 2010-12)
	count = 0
	crimes = get_police_data( '/crimes-street/all-crime', {'poly': place_string, 'date': month_string})
	if crimes is None or len(crimes) == 0:
		#print "INF: No crimes to load"
		return 0
	else:
		for crime in crimes:
			count = count + 1
			#if count >= 384: # debug code
			#print "DBG: Loading crime " + str(count) + "/" + str(len(crimes)) + " : " + str(crime['id'])
			#print repr(crime)
				
			crime_id = mysql_function('post_police_crime', [\
					crime['category'], \
					crime['id'], \
					crime['persistent_id'], \
					crime['context'], \
					crime['month'], \
					crime['location_type'] \
						if 'location_type' in crime else None, \
					crime['location_subtype'] \
						if 'location_subtype' in crime else None,\
					crime['location']['street']['id'], \
					crime['location']['street']['name'], \
					crime['location']['latitude'] \
						if 'latitude' in crime['location'] else None, \
					crime['location']['longitude'] \
						if 'latitude' in crime['location'] else None, \
					crime['outcome_status']['category'] \
						if ('outcome_status' in crime and crime['outcome_status'] is not None and 'category' in crime['outcome_status']) else None, \
					crime['outcome_status']['date'] \
						if ('outcome_status' in crime and crime['outcome_status'] is not None and 'date' in crime['outcome_status']) else None \
				])
	if crimes is not None:
		return len(crimes)
	else:
		return 0

# loads outcome data
def load_outcomes(place, month):
	# check if month is valid
	month = standardise_date(month)

	if region is None or month is None or datetime.strptime(month, '%Y-%m-%d') > datetime.today():
		print "ERR: load_crime : must specify both region and month (YYYY-MM-DD) and month must be in the past."
		return 0

	# convert geography params into forms used by police API
	load_outcome_cursor = db.cursor()
	query = "select convert_geometry_to_police_string(mbr_polygon )from place where name = '" + place + "'"
	try:
		load_outcome_cursor.execute(query)
	except:
		print('ERR: Unable to get run query "' + query + '"')
		# sys.exit(1)
		return 0
	place_string = load_outcome_cursor.fetchone()
	load_outcome_cursor.close()
	month_string = '-'.join((month.split('-')[0], month.split('-')[1]))

	# load crime data (returns no data before 2010-12)
	count = 0
	outcomes = get_police_data( '/outcomes-at-location', {'poly': place_string, 'date': month_string})
	if outcomes is None or len(outcomes) == 0:
		#print "INF: No outcomes to load"
		return 0
	else:
		for outcome in outcomes:
			count = count + 1
			# print "DBG: Loading outcome for crime " + str(count) + "/" + str(len(outcomes)) + " : " + str(outcome['crime']['id'])
			# print outcome
			
			outcome_id = mysql_function('post_police_outcome', [\
					outcome['category']['code'], \
					outcome['category']['name'] \
						if 'name' in outcome['category'] else None, \
					outcome['date'], \
					outcome['person_id'] \
						if 'person_id' in outcome else None, \
					outcome['crime']['category'] \
						if ('crime' in outcome and outcome['crime'] is not None and 'category' in outcome['crime']) 		else None, \
					outcome['crime']['id'] \
						if ('crime' in outcome and outcome['crime'] is not None and 'id' in outcome['crime']) 			else None, \
					outcome['crime']['persistent_id'] \
						if ('crime' in outcome and outcome['crime'] is not None and 'persistent_id' in outcome['crime']) 	else None, \
					outcome['crime']['context'] \
						if ('crime' in outcome and outcome['crime'] is not None and 'context' in outcome['crime']) 		else None, \
					outcome['crime']['month'] \
						if ('crime' in outcome and outcome['crime'] is not None and 'month' in outcome['crime']) 		else None, \
					outcome['crime']['location_type'] \
						if ('crime' in outcome and outcome['crime'] is not None and 'location_type' in outcome['crime']) 	else None, \
					outcome['crime']['location_subtype'] \
						if ('crime' in outcome and outcome['crime'] is not None and 'location_subtype' in outcome['crime']) 	else None, \
					outcome['crime']['location']['street']['id'] \
						if (	'crime' 	in outcome 				and outcome['crime'] 				is not None and \
							'location' 	in outcome['crime'] 			and outcome['crime']['location'] 		is not None and \
							'street' 	in outcome['crime']['location'] 	and outcome['crime']['location']['street'] 	is not None and \
							'id'		in outcome['crime']['location']['street'] \
						) else None, \
					outcome['crime']['location']['street']['name'] \
						if (	'crime' 	in outcome 				and outcome['crime'] 				is not None and \
							'location' 	in outcome['crime'] 			and outcome['crime']['location'] 		is not None and \
							'street' 	in outcome['crime']['location'] 	and outcome['crime']['location']['street'] 	is not None and \
							'name'		in outcome['crime']['location']['street'] \
						) else None, \
					outcome['crime']['location']['latitude'] \
						if (	'crime' 	in outcome 				and outcome['crime'] 			is not None and \
							'location' 	in outcome['crime'] 			and outcome['crime']['location'] 	is not None and \
							'latitude'	in outcome['crime']['location'] \
						) else None, \
					outcome['crime']['location']['longitude'] \
						if (	'crime' 	in outcome 				and outcome['crime'] 			is not None and \
							'location' 	in outcome['crime'] 			and outcome['crime']['location'] 	is not None and \
							'longitude'	in outcome['crime']['location'] \
						) else None \
				])
	if outcomes is not None:
		return len(outcomes)
	else:
		return 0

# loads stops data
# https://data.police.uk/api/stops-street?poly=52.268,0.543:52.794,0.238:52.130,0.478&date=2015-01
def load_stops(place, month):
	# check if month is valid
	month = standardise_date(month)

	if region is None or month is None or datetime.strptime(month, '%Y-%m-%d') > datetime.today():
		print "ERR: load_stops : must specify both region and month (YYYY-MM-DD) and month must be in the past."
		return 0

	# convert geography params into forms used by police API
	load_stop_cursor = db.cursor()
	query = "select convert_geometry_to_police_string(mbr_polygon )from place where name = '" + place + "'"
	try:
		load_stop_cursor.execute(query)
	except:
		print('ERR: Unable to get run query "' + query + '"')
		# sys.exit(1)
		return 0
	place_string = load_stop_cursor.fetchone()
	load_stop_cursor.close()
	month_string = '-'.join((month.split('-')[0], month.split('-')[1]))

	# load crime data (returns no data before 2010-12)
	count = 0
	stops = get_police_data( '/stops-street', {'poly': place_string, 'date': month_string})
	if stops is None or len(stops) == 0:
		#print "INF: No stops to load"
		return 0
	else:
		for stop in stops:
			count = count + 1
			#print "DBG: Loading stop " + str(count) + "/" + str(len(stops))
			#print stop
			
			stop_id = mysql_function('post_police_stop', [\
					stop['datetime'], \
					stop['outcome_linked_to_object_of_search'] if 'outcome_linked_to_object_of_search' in stop else None, \
					stop['type'], \
					stop['operation'] \
						if 'operation' in stop else None, \
					stop['object_of_search'] \
						if 'object_of_search' in stop else None, \
					stop['operation_name'] \
						if 'operation_name' in stop else None, \
					stop['removal_of_more_than_outer_clothing'] \
						if 'removal_of_more_than_outer_clothing' in stop else None, \
					#stop['outcome'] \
					#	if 'outcome' in stop else None, \
					stop['outcome_object']['name'] \
						if ('outcome_object' in stop and stop['outcome_object'] is not None and 'name' in stop['outcome_object']) else None, \
					stop['legislation'] \
						if 'legislation' in stop else None, \
					stop['involved_person'] \
						if 'involved_person' in stop else None, \
					stop['location']['street']['id'] \
						if ('location' in stop and stop['location'] is not None and 'street' in stop['location'] and stop['location']['street'] is not None and 'id' in stop['location']['street']) else None, \
					stop['location']['street']['name'] \
						if ('location' in stop and stop['location'] is not None and 'street' in stop['location'] and stop['location']['street'] is not None and 'name' in stop['location']['street']) else None, \
					stop['location']['latitude'] \
						if ('location' in stop and stop['location'] is not None and 'latitude' in stop['location']) else None, \
					stop['location']['longitude'] \
						if ('location' in stop and stop['location'] is not None and 'longitude' in stop['location']) else None, \
					stop['gender'] \
						if 'gender' in stop else None, \
					stop['self_defined_ethnicity'] \
						if 'self_defined_ethnicity' in stop else None, \
					stop['officer_defined_ethnicity'] \
						if 'officer_defined_ethnicity' in stop else None, \
					stop['age_range'] \
						if 'age_range' in stop else None \
				])
	if stops is not None:
		return len(stops)
	else:
		return 0

### MAIN ###

# manage commandline args
try:
	opts, args = getopt.getopt(sys.argv[1:], "r:u:p:h:d:o:", ["region=", "user=", "password=", "host=", "database=", "options=" ])
except getopt.GetoptError as err:
	print(err)
	sys.exit(2)

# get commandline options
for o, a in opts:
	if o in ("-r", "--region"):
		region = a
	elif o in ("-u", "--user"):
		user = a
	elif o in ("-p", "--password"):
		password = a
	elif o in ("-h", "--host"):
		host = a
	elif o in ("-d", "--database"):
		database = a
	elif o in ("-o", "--options"):
		options = a

# check for required parms
if region is None or user is None or password is None or host is None or database is None:
	print('ERR: Specify all parameters')
	sys.exit(1)

# test connection to database
try:
	db = mysql.connector.connect(user=user, password=password, host=host, database=database, charset='utf8', get_warnings=True, raise_on_warnings=True)
except:
	print('ERR: Unable to connect to database "' + database + '" with user "' + user + '"' )
	sys.exit(1)
#print('INF: Connection to DB : OK')

# test connection to police data API
# police data website is *extremely* flaky
police_data_last_updated = get_police_data('crime-last-updated', None)
if police_data_last_updated is None or len(police_data_last_updated) == 0:
	print('ERR: Unable to access police data.')
	sys.exit(1)
else :
	police_data_last_updated = standardise_date(police_data_last_updated[0]['date'])
#print('INF: Connection to police API : OK')

# default data load start date (earliest police data 2015-01)
# default_start = standardise_date(datetime.strftime(datetime.today().date() - timedelta(days=1095), '%Y-%m-01')) # 3 yr
# default_start = standardise_date(datetime.strftime(datetime.today().date() - timedelta(days=1825), '%Y-%m-01')) # 5 yr
default_start = standardise_date(datetime.strftime(datetime.today().date() - timedelta(days=365), '%Y-%m-01')) # 1 yr
#default_start =  '2015-06-01' # fixed date
#print "DBG: default_start : " + default_start

# get last update value (assume default_start if blank)
local_data_last_updated = standardise_date(mysql_function('get_variable', ['crime-last-updated']))
if local_data_last_updated is None or len(local_data_last_updated) == 0:
	local_data_last_updated = default_start
	mysql_procedure('post_variable', ['crime-last-updated', default_start])

# exit here if already updated
# police_data_last_updated is always in the form 'YYYY-MM-01' which actually means 'YYYY-MM-DD' where DD is last day of month
if datetime.strptime(local_data_last_updated, '%Y-%m-%d') >= datetime.strptime(police_data_last_updated, '%Y-%m-%d'):
	# print('INF: Already up to date; nothing to do.')
	sys.exit(0)

# test police data load isnt already running
# mysql_procedure('delete_variable', ['police-data-load'])
if mysql_function('get_variable', ['police-data-load']) is not None:
	print('ERR: Police data load is already running (or failed previously)' )
	sys.exit(1)
mysql_procedure('post_variable', ['police-data-load', 'started'])

# test region specified exists
if region is not None and mysql_function('exists_place', ['name', region ]) == 0 :
	print('ERROR : Region "' + region + '" has not been loaded into the database')
	sys.exit(1)
# print('INF: Region specified : OK' )

# debug quit before loading
# sys.exit(0)

# load force(s) for region
if 'no-force-load' not in options:
	mysql_procedure('put_variable', ['police-data-load', 'loading force and neighbourhood data' ])
	if load_force(region) is None:
		print('Unable to load forces for region "' + region + '"')
		sys.exit(1)
	# print('INF: Force data loaded : OK')

loop = 1 # safety net
maxloop = 100 # safety net

# wind through each month (currently starts loop on latest already-loaded month)
# while datetime.strptime(local_data_last_updated, '%Y-%m-%d') <= datetime.strptime(police_data_last_updated, '%Y-%m-%d') \

# starting point = three years ago (minus 1 month, which is added back on at top of loop)
month_to_load = default_start

# wind through each month (currently starts loop on 3 years ago, to catch up on later changes to historical data)
while datetime.strptime(month_to_load, '%Y-%m-%d') <= datetime.strptime(police_data_last_updated, '%Y-%m-%d') \
	and datetime.strptime(month_to_load, '%Y-%m-%d') <= datetime.today() \
	and loop <= maxloop:
	
	# print "DBG: Loop : " + str(loop) + "/" + str(maxloop) + " (" + month_to_load + " / " + police_data_last_updated + ")"
	crimes_loaded = 0

	if 'no-category-load' not in options:
		# print "INF: Loading categories for month '" + month_to_load + "'."
		mysql_procedure('put_variable', ['police-data-load', 'loading categories ' + ' (' + month_to_load + ' / ' + police_data_last_updated + ')' ])
		categories_loaded = load_categories(month_to_load)
		# print "INF: Loaded " + str(categories_loaded) + " category records for '" + month_to_load + "'."

	# debug quit
	#sys.exit(0)

	if 'no-crime-load' not in options:
		# print "INF: Loading crime data for region '" + region + "' and month '" + month_to_load + "'."
		mysql_procedure('put_variable', ['police-data-load', 'loading crimes ' + ' (' + month_to_load + ' / ' + police_data_last_updated + ')' ])
		crimes_loaded = load_crimes(region, month_to_load)
		# print "INF: Loaded " + str(crimes_loaded) + " crime records for '" + month_to_load + "'."
	
	# only continue if crime data was loaded (note this can cause issues when loading a new DB and crime-last-updated is so old that theres no data for it)
	# if crimes_loaded is not None and crimes_loaded > 0 :
	if 'no-outcome-load' not in options:
		# print "INF: Loading outcome data for region '" + region + "' and month '" + month_to_load + "'."
		mysql_procedure('put_variable', ['police-data-load', 'loading outcomes ' + ' (' + month_to_load + ' / ' + police_data_last_updated + ')' ])
		outcomes_loaded = load_outcomes(region, month_to_load)
		# print "INF: Loaded " + str(outcomes_loaded) + " outcome records for '" + month_to_load + "'."

	if 'no-stop-load' not in options:
		# print "INF: Loading stop data for region '" + region + "' and month '" + month_to_load + "'."
		mysql_procedure('put_variable', ['police-data-load', 'loading stops ' + ' (' + month_to_load + ' / ' + police_data_last_updated + ')' ])
		stops_loaded = load_stops(region, month_to_load)
		# print "INF: Loaded " + str(stops_loaded) + " stop records for '" + month_to_load + "'."

	# update crime-last-updated value if any crimes have been loaded
	if datetime.strptime(month_to_load, '%Y-%m-%d') > datetime.strptime(local_data_last_updated, '%Y-%m-%d'):
		# print "INF: Logging variable 'crime-last-updated' = " + month_to_load + "'."
		mysql_procedure('put_variable', ['crime-last-updated', month_to_load])

	loop = loop + 1
	month_to_load = standardise_date( datetime.strftime( datetime.strptime( month_to_load, '%Y-%m-%d' ) + relativedelta.relativedelta(months=1), '%Y-%m-%d' ) )

# perform data load sanity check
mysql_procedure('delete_variable', ['crime-load-sanity'])
mysql_procedure('post_variable', ['crime-load-sanity',  mysql_function('police_crime_sanity_check','') ])

# mark that load has completed
mysql_procedure('delete_variable', ['police-data-load'])
