import json
import logging
import os
import time
import sqlite3 as sl

import tweepy
from dotenv import load_dotenv

# config
with open("config.json") as cfgfile:
    CFG = json.load(cfgfile)

load_dotenv()

LOGFILE=CFG["log-file"]
ACCOUNT_NAME=CFG["account-name"]

CONS_KEY=os.getenv('TWT_CONSUMER_APIKEY')
CONS_SEC=os.getenv('TWT_CONSUMER_APISECRET')
AUTH_ACC=os.getenv('TWT_AUTH_ACCESSTOKEN')
AUTH_SEC=os.getenv('TWT_AUTH_SECRET')
BEARER=os.getenv('TWT_BEARER')

logging.basicConfig(filename=LOGFILE, 
    encoding='utf-8', 
    level=logging.INFO, 
    format='%(asctime)s %(message)s',
    datefmt='[%a %d-%m-%y %H:%M:%S]'
    )

# references
auth = tweepy.OAuthHandler(CONS_KEY, CONS_SEC)
auth.set_access_token(AUTH_ACC, AUTH_SEC)
twapi = tweepy.API(auth, wait_on_rate_limit=True)
client = tweepy.Client(BEARER)

timenow = str(int(time.time()))

try:
    twapi.verify_credentials()
    print("Authentication OK")
except Exception as e:
    logging.error(f"Error during authentication: {e}")

# get user ID, latest tweet, tweet info from account name
user = twapi.get_user(screen_name=ACCOUNT_NAME)
user_id = user.id_str

logging.info(f"Configured for @{ACCOUNT_NAME} with ID {user_id}.")
print(f"Configured for @{ACCOUNT_NAME} with ID {user_id}.")

dbcon = sl.connect("database.db")
dbname = ACCOUNT_NAME

# with dbcon:
#     dbcon.execute(f"""
#         CREATE TABLE {dbname} (
#             tweetid TEXT PRIMARY KEY,
#             username TEXT,
#             timestamp TEXT
#         );
#     """)

def db_insert(data1: list, data2: list, timestamp: str, db: sl.Connection):
    # TODO: error handler decorator
    sqlstring = f'INSERT INTO {ACCOUNT_NAME} (tweetid, username, timestamp) values(?, ?, ?)'
    data = list(zip(data1, data2, [timestamp] * len(data1)))
    try:
        with db:
            db.executemany(sqlstring, data)
    except sl.IntegrityError as e:
        logging.warning(f"{e}")
        print("IntegrityError; skipped insert")

def db_query(field: str, key: str, operator: str, value, db: sl.Connection):
    # TODO: error handler decorator
    cursor = db.cursor()
    sqlstring = f'SELECT {field} FROM {dbname} WHERE {key} {operator} {value}'
    with db:
        data = cursor.execute(sqlstring)
        return [i[0] for i in data]

def db_squery(field: str, db: sl.Connection):
    # TODO: error handler decorator
    cursor = db.cursor()
    sqlstring = f'SELECT {field} FROM {dbname}'
    with db:
        data = cursor.execute(sqlstring)
        return [i[0] for i in data]

response = client.get_users_tweets(user_id, max_results=5)

tweet_ids = [i.id for i in response.data]
tweet_urls = [f"https://twitter.com/{ACCOUNT_NAME}/status/{i}" for i in tweet_ids]
tweet_object = [twapi.lookup_statuses([i], include_entities=False, trim_user=True) for i in tweet_ids]
tweet_info_json = [i[0]._json for i in tweet_object]

unamelist = [ACCOUNT_NAME] * 5
db_insert(tweet_ids, unamelist, timenow, dbcon)
new_tweets_ids = []

for i in tweet_ids:
    i = str(i)
    if i in db_squery("tweetid", dbcon):
        print(f"Tweet ID '{i}' already exists, no new tweets detected")
    else:
        print(f"Tweet ID '{i}' not found, new tweet posted!")
        new_tweets_ids.append(i)

print(new_tweets_ids)
db_insert(new_tweets_ids, [ACCOUNT_NAME]*len(new_tweets_ids), timenow, dbcon)