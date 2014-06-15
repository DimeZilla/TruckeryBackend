#Authored by Joshua Diamond 2014-05-18
#This script connects retrieves newsfeed data from twitter and offloads it into a postgresql database
#dependencies: TwitterAPI, psycopg2

from TwitterAPI import TwitterAPI #Gets twitter api functions
import psycopg2 #for handling connections with postgresql database
from time import strftime #for time loggin
import psycopg2.extensions as pe

logging = strftime("%Y-%m-%d %H:%M:%S") + ":" + " STARTNG TWIITTER API IMPORT" 
try:
	import json
except ImportError:
	import simplejson as json


api = TwitterAPI('','','','')

response = api.request('statuses/home_timeline', {'screen_name':'sorryIcom'})

conn = psycopg2.connect(database="truckery", user="postgres", password="", host="127.0.0.1", port="5432")
if conn:
	logging += "; Opened database successfully"
else:
	logging += "; Connection unsuccessful"
	print logging
	exit()

curr = conn.cursor()
# Check to make sure table exists
qCreate = "CREATE TABLE IF NOT EXISTS api_inc(id SERIAL, user_id BIGINT, user_sn VARCHAR(30), tweet_text VARCHAR(150), tweet_id BIGINT, tweet_geo VARCHAR(150), tweet_place VARCHAR(150));"
try:
	curr.execute(qCreate)
except psycopg2.ProgrammingError:
	logging += "; qCreate errored out with error"
	print logging
	exit()

conn.commit()

for item in response.get_iterator():
	user_id = item["user"]["id"]
	user_sn = str(item['user']["screen_name"].encode('utf-8'))
	tweet_text = pe.adapt((str(item["text"].encode('utf-8')).replace("'", "\\'")).replace('"','\\"'))
	tweet_id = item["id"]
	created = str(item["created_at"].encode('utf-8'))
	try:
		geo = str(item["geo"].encode('utf-8'))
	except AttributeError:
		geo = 'n/a'
	try:
		place = str(item["place"].encode('utf-8'))
	except AttributeError:
		place = 'n/a'

	qInsert = """INSERT INTO api_inc (user_id,user_sn,tweet_text,tweet_id,tweet_geo,tweet_place, created)
	VALUES (%d,'%s',%s,%d,'%s', '%s', '%s');
	""" % (user_id, user_sn, tweet_text, tweet_id, geo, place, created)
	#((((str(item).encode('utf-8')).replace('"','\\"')).replace("[","")).replace("u'","")).replace("'","")
	try:
		curr.execute(qInsert)
	except psycopg2.ProgrammingError:
		logging += "; qInsert errored out with ProgrammingError" 
		print logging
		exit()

#Getting the number of rows to insert into twitter_fact for logging purposes
qInsertCount = "select count(*) as number from api_inc WHERE tweet_id NOT IN (SELECT tweet_id FROM twitter_fact);"
try:
	curr.execute(qInsertCount)
except psycopg2.ProgrammingError:
	logging += "; qInsertCount errored out with ProgrammingError - moving on"
	

numInserted = curr.fetchone()
logging += "; Inserting " + str(numInserted[0]) + " into twitter_fact"

#inserting unique rows into twitter_fact
qTwitFact = """INSERT INTO twitter_fact(user_id, user_sn, tweet_text, tweet_id, tweet_geo, tweet_place, created) 
				(select user_id, user_sn, tweet_text, tweet_id, tweet_geo, tweet_place, created from api_inc 
					WHERE tweet_id NOT IN (SELECT tweet_id FROM twitter_fact)); """
try:
	curr.execute(qTwitFact);
except psycopg2.ProgrammingError:
	logging += "; qTwitfact errored out with ProgrammingError at"
	print logging
	exit()


#commit changes 
conn.commit()
logging += "; ENDING TWITTER API IMPORT"
print logging