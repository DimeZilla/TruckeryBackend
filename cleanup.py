#This is a cleanup script to dump old Twitter API data that is older than 5 days.
#Authored by Joshua Diamond
#05/28/3014

import psycopg2 #for handling connections with postgresql database
from time import strftime #for time loggin
import psycopg2.extensions as pe

conn = psycopg2.connect(database="truckery", user="postgres", password="", host="127.0.0.1", port="5432")
if conn:
	logging += "; Opened database successfully"
else:
	logging += "; Connection unsuccessful"
	print logging
	exit()

curr = conn.cursor()

logging = strftime("%Y-%m-%d %H:%M:%S") + ": "
#cleaning up api_inc and restarting id sequencig
qCleanUpinc = """DELETE FROM api_inc;
		DELETE FROM twitter_fact WHERE timestamp < now() - INTERVAL '3 DAYS';
		ALTER SEQUENCE api_inc_id_seq RESTART WITH 1; """
try:
	curr.execute(qCleanUpinc)
	logging += "Successfully cleaned up the Twitter table"
except psycopg2.ProgrammingError:
	logging += "qCleanUpinc errored out with ProgrammingError... soooo....Cleanup it up yourself!"

conn.commit()

print logging