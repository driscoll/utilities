# -*- coding: utf-8 -*-
"""
Create a collection of only those tweets that contain links to YouTube videos

Kevin Driscoll, 2013
"""

from collections import Counter
import datetime
import longurl
import pymongo
import tweetutils

if __name__=="__main__":

    database = ""
    input_collection = ""
    output_collection = ""

    observation_periods = [
        (datetime.datetime(2012, 10, 1),
        datetime.datetime(2012, 10, 4)), 
        (datetime.datetime(2012, 10, 15),
        datetime.datetime(2012, 10, 18)),
        (datetime.datetime(2012, 10, 21),
        datetime.datetime(2012, 10, 24)),
    ]

    # Init LongURL API wrapper
    expander = longurl.LongURL()

    # Init connection to Mongo database instance
    mongo = pymongo.Connection()
    db = mongo[database]
    collection = db[input_collection]
    print "Indexing", input_collection, "on postedTimeObj"
    collection.ensure_index('postedTimeObj')
    print "Dropping", output_collection 
    db.drop_collection(output_collection)
    
    print "Opening youtube_progress.txt to track tweet IDs..."
    progressf = open('youtube_progress.txt', 'wb')

    print "Parsing tweets in search of YouTube links..." 
    c = Counter()
    for start, end in observation_periods:
        query = {'postedTimeObj':
                    {'$gte': start,
                    '$lt': end}}
        projection = {'_id': True,
                        'twitter_entities': True,
                        'body': True,
                        'postedTimeObj': True,
                        'actor.id_str': True,
                        'actor.preferredUsername': True}
        cursor = collection.find(query, projection, timeout=False)
        
        tweet_ids = []
        for tweet in cursor:
            tweet_ids.append(tweet.get('_id'))
            c['tweet'] += 1
            if not c['tweet'] % 5000:
                for twid in tweet_ids:
                    progressf.write(twid)
                    progressf.write('\n')
                tweet_ids = []
                progressf.flush()
            for u in tweet['twitter_entities']['urls']:
                if u['expanded_url']:
                    shorturl = u['expanded_url']
                else:
                    shorturl = u['url']
                youtube_id = tweetutils.parse_youtube_id(shorturl)
                if youtube_id:
                    c['expanded'] += 1
                else:
                    try:
                        qurl = expander.expandable(shorturl)
                    except:
                        # TODO this is pretty bunk
                        # but the expander throws a ton of diff errors 
                        # and I'm just trying to get work done! =)
                        # It's okay to lose a few
                        c['exceptions'] += 1
                        qurl = None
                    if qurl:
                        try:
                            lengthened = expander.expand(shorturl, qurl) 
                        except:
                            # TODO this is pretty bunk
                            # but the expander throws a ton of diff errors 
                            # and I'm just trying to get work done! =)
                            # It's okay to lose a few
                            c['exceptions'] += 1
                            lengthened = ''
                        youtube_id = tweetutils.parse_youtube_id(lengthened)
                        if youtube_id:
                            c['longurl'] += 1
                if youtube_id:
                    tweet['youtube_id'] = youtube_id
                    # TODO DEBUG
                    # print youtube_id, tweet['actor']['preferredUsername'], tweet['body']
                    db[output_collection].insert(tweet)
                    c['insert'] += 1
                    if not c['insert'] % 100:
                        print c['insert'], "out of", c['tweet'], tweet['body']

    progressf.close()
    print c
                


