# -*- coding: utf-8 -*-
"""
Insert tweet(s) in Activity Streams 
format into MongoDB


From a local file:
$ cat my_tweets*.json | python mongo_insert.py --rules=rules.txt db_name collection 2> failed_inserts.txt

From a remote file:
$ ssh username@server 'cat /home/username/mydata/tweets*' | python mongo_insert.py --rules=rules.txt db_name collection 2> failed_inserts.txt

From an HTTP stream:
$ curl | python mongo_insert.py --rules=rules.txt db_name collection 2> failed_inserts.txt


Kevin Driscoll, 2012, 2013

"""

from datetime import datetime
from mongoutils import *
from tweetutils import *
import argparse
import json
import pymongo
import sys
import rfc3339

def init(HOST, PORT, DB):
    conn = pymongo.Connection(HOST, int(PORT))
    return conn[DB]

def insert(obj, db, collection):
    return db[collection].insert(obj)

def save(obj, db, collection):
    return db[collection].save(obj)

if __name__=="__main__":

    # default values
    HOST        = 'localhost'
    PORT        = '27017'
   
    # parse args
    parser = argparse.ArgumentParser(description='Insert JSON objects from stdin into MongoDB')
    parser.add_argument('--host', type=str, default=HOST, help='Hostname of primary MongoDB node')
    parser.add_argument('--port', type=str, default=PORT, help='Port number of primary MongoDB node')
    parser.add_argument('--rules', type=str, default='', help='Path to file with Gnip rules, one per line')
    parser.add_argument('db', metavar='DB', type=str, help='Database to insert into')
    parser.add_argument('collection', metavar='COLLECTION', type=str, help='Collection in the database to insert into')
    
    args = parser.parse_args()
    # Check if there are rules to read 
    rules = []
    print args.rules
    if os.path.isfile(args.rules):
        print "Trying to read rules from: {0}".format(args.rules)
        f = open(args.rules, 'rb')
        for line in f:
            rule = line.strip()
            if rule:
                rules.append(rule)
        if not rules:
            print "Didn't find any rules."
            sys.exit()
        else:
            print "Found {0} rules.".format(len(rules))

    print 'Inserting tweets from stdin into:'
    print '\tHost       :\t{0}'.format(args.host)
    print '\tPort       :\t{0}'.format(args.port)
    print '\tDatabase   :\t{0}'.format(args.db)
    print '\tCollection :\t{0}'.format(args.collection)
    # print 'Insert\tFailure\tLast tweet timestamp'
    print 'Insert\tValueError\tNotGnip\tLast tweet timestamp'

    # connect to db
    db = init(args.host, args.port, args.db)

    # read tweets in from stdin
    inserts  = 0
    failures = 0
    valueerror = 0
    notgnip = 0
    last_tweet  = ''

    # keep reading until there are no lines left
    line = sys.stdin.readline().strip()
    while line:
        # try to make a json obj out of it
        try:
            tweet = json.loads(line)

        # If you get a ValueError, write the
        # line to stderr, and get a new line
        except ValueError:
            sys.stderr.write(line)
            sys.stderr.write('\n')
            valueerror+= 1 
            if valueerror % 500 == 0:
                print '{0}\t{1}\t{2}\t{3}'.format(inserts, valueerror, notgnip, last_tweet)
            line = sys.stdin.readline().strip()
            continue

        if not 'gnip' in tweet.keys():
            notgnip += 1 
            if notgnip % 500 == 0:
                print '{0}\t{1}\t{2}\t{3}'.format(inserts, valueerror, notgnip, last_tweet)
            line = sys.stdin.readline().strip()
            continue

        # Check if it's matching a Gnip rule we care about: 
        if rules:
            if not tweet_matches_rules(tweet, rules):
                line = sys.stdin.readline().strip()
                continue

        # If it worked, interpret the postedTime str
        # and create a Python datetime object
        # MongoDB will store this as a native date obj
        if 'postedTime' in tweet:
            dt = convert_date(tweet['postedTime'])
            tweet['postedTimeObj'] = dt
            last_tweet = dt.isoformat()

        if 'postedTime' in tweet['actor']):
            dt = convert_date(tweet['actor']['postedTime'])
            tweet['actor']['postedTimeObj'] = dt

        if 'postedTime' in tweet['object']):
            dt = convert_date(tweet['object']['postedTime'])
            tweet['object']['postedTimeObj'] = dt

        if 'actor' in tweet['object']:
            if 'postedTime' in tweet['object']['actor']:
                dt = convert_date(tweet['object']['actor']['postedTime'])
                tweet['object']['actor']['postedTimeObj'] = dt

        # Next, create fields for user_id and tweet_id
        # Use id_str as teh _id field for mongoDB
        tweet['id_str'] = extract_tweet_id(tweet['id'])
        tweet['_id'] = tweet['id_str']
        tweet['actor']['id_str'] = extract_user_id(tweet['actor']['id'])
        if 'id' in tweet['object']):
            tweet['object']['id_str'] = extract_tweet_id(tweet['object']['id'])
        if 'actor' in tweet['object']:
            if 'id' in tweet['object']['actor']:
                tweet['object']['actor']['id_str'] = extract_tweet_id(tweet['object']['actor']['id'])

        # Now insert it into the collection 
        # insert(tweet, db, args.collection)
        save(tweet, db, args.collection)
        inserts += 1
        if inserts % 50000 == 0:
            print '{0}\t{1}\t{2}\t{3}'.format(inserts, valueerror, notgnip, last_tweet)

        # Finally, get a new line
        line = sys.stdin.readline().strip()

    # Short summary to stdout
    print
    print 'Totals:'
    print 'Inserts\tFailures\tDate of last tweet inserted'
    print '{0}\t{1}\t{2}\t{3}'.format(inserts, valueerror, notgnip, last_tweet)
