# Python script to merge collections in MongoDB and 
# store it in a new collection

from pymongo import MongoClient
from pymongo import TEXT
from pymongo.errors import DuplicateKeyError
from pprint import pprint
import logging
import json
import requests
import csv
from bs4 import BeautifulSoup

# List of collection names to merge
COLLECTION_LIST = ['stream_tweets']
# Name of the new collection
NEW_COLLECTION = 'stranger_things'
# Name of the Mongo Database
DB_NAME = 'tweetCorpus'

# Configure logging
log_file = "mergeCollections_log.log"
# Start logging
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s %(message)s')

def check_active_user(screen_name):
    # check if the tweet is coming from an active user
    url = 'https://twitter.com/{0}'.format(screen_name)
    try:
        req = requests.get(url)
        req.raise_for_status()
    except requests.exceptions.HTTPError as Err:
        logging.debug(Err)
        return 0
    soup = BeautifulSoup(req.text, 'html.parser')
    if (soup.find('div', {'class':'flex-module error-page clearfix'}) 
        is not None):
        logging.info('''Account suspended for the user with 
        screen_name: @{0}'''.format(screen_name))
        return 0
    return 1
    

if __name__ == "__main__":
    db = MongoClient()[DB_NAME]
    # Create index on tweetid
    db[NEW_COLLECTION].create_index([('id_str', TEXT)], 
                                    unique=True)
    total_count = 0
    for collection in COLLECTION_LIST:
        coll = db[collection]
        logging.info('''Adding {0} collection to {1} 
                      collection..'''.format(collection, NEW_COLLECTION))
        count= 0
        discarded_user_list = list()
        cursor = coll.find({}, no_cursor_timeout=True)
        for tweet in cursor:
            tweeted_user = tweet['user']['screen_name']
            if tweeted_user not in discarded_user_list :
                if not check_active_user(tweeted_user):
                    discarded_user_list.append(tweeted_user)
                    continue
                try:
                    db[NEW_COLLECTION].insert_one(tweet)
                except DuplicateKeyError as E:
                    logging.debug(E)
                    continue
                count += 1
                if count%1000==0:
                    logging.info('''Added {0} records from {1} collection...
                                '''.format(count, collection))
        cursor.close()
        total_count += count
        logging.info('''Finished adding {0} collection to {1} 
                    collection..\n'''.format(collection, NEW_COLLECTION))
        logging.info('''Added {0} records from {1} collection...
                                '''.format(total_count, NEW_COLLECTION))    
    logging.info('''Number of users in discarded_user_list is 
                 {0}...'''.format(len(discarded_user_list)))
    
    with open('discarded_user_list.csv','w') as f:
        writer = csv.writer()
        csv.writer(f).writerows(zip(discarded_user_list))


