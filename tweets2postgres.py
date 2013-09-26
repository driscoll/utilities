# -*- coding: utf-8 -*-
"""
Insert tweet(s) in Activity Streams format into PostgreSQL 

From a local file:
$ cat my_tweets*.json | python tweets2postgres.py --rules=rules.txt db_name collection 2> failed_inserts.txt

From a remote file:
$ ssh username@server 'cat /home/username/mydata/tweets*' | python tweets2postgres.py --rules=rules.txt db_name collection 2> failed_inserts.txt

From an HTTP stream:
$ curl | python tweets2postgres.py --rules=rules.txt db_name collection 2> failed_inserts.txt


Kevin Driscoll, 2012, 2013

"""

from tweetutils import *
import argparse
import json
import os
import sys

def next_line():
    return sys.stdin.readline().strip()

def parse_tweets_columns(tweet):
    if 'text' in tweet:
        return parse_tweets_columns_native(tweet)
    elif 'body' in tweet:
        return parse_tweets_columns_activity_streams(tweet)
    else:
        return {}

def parse_tweets_columns_activity_streams(tweet):
    id_str = extract_tweet_id(tweet.get('id', ''))
    posted_time = from_postedTime(tweet['postedTime'])
    rt = parse_retweet(tweet)
    actor_id = extract_user_id(tweet.get('actor', {}).get('id', ''))
    inreplyto_id = tweet.get('inReplyTo', {}).get('link', '').rsplit('/', 1)[-1]
    urls = tweet.get('twitter_entities',{}).get('urls', [])
    hashtags = tweet.get('twitter_entities',{}).get('hashtags', [])
    user_mentions = tweet.get('twitter_entities',{}).get('user_mentions', [])
    return {'id': id_str,
            'posted_time': posted_time,
            'verb': tweet.get('verb', ''),
            'retweet_id': rt.get('retweeted_status_id_str', ''),
            'actor_id': actor_id, 
            'body': tweet.get('body', ''),
            'generator': tweet.get('generator', {}).get('displayName', ''),
            'inreplyto_id': inreplyto_id,
            'body_tsv': None,
            'is_retweet': int(bool(rt)),
            'edited_retweet': int(rt.get('edited', False)),
            'urls': json.dumps(urls),
            'hashtags': json.dumps(hashtags),
            'user_mentions': json.dumps(user_mentions),
            'num_urls': len(urls),
            'num_hashtags': len(hashtags),
            'num_user_mentions': len(user_mentions)}

def parse_tweets_columns_native(tweet):
    created_at = tweet.get('created_at')
    dt = datetime.datetime.strptime(created_at, GNIP_DATETIME_FORMAT)
    user = tweet.get('user', {})
    source_name, source_url = extract_source(tweet.get('source', ''))
    urls = tweet.get('entities',{}).get('urls', [])
    hashtags = tweet.get('entities',{}).get('hashtags', [])
    user_mentions = tweet.get('entities',{}).get('user_mentions', [])
    rt = parse_retweet(tweet)
    return {'id': tweet.get('id_str', ''),
            'posted_time': dt,
            'verb': '',
            'retweet_id': rt.get('retweeted_status_id_str', ''),
            'actor_id': user.get('id_str', ''),
            'body': tweet.get('text', ''),
            'generator': source_name,
            'inreplyto_id': tweet.get('in_reply_to_status_id_str', ''),
            'body_tsv': None, 
            'is_retweet': int(bool(rt)),
            'edited_retweet': int(rt.get('edited', False)),
            'urls': json.dumps(urls),
            'hashtags': json.dumps(hashtags),
            'user_mentions': json.dumps(user_mentions),
            'num_urls': len(urls),
            'num_hashtags': len(hashtags),
            'num_user_mentions': len(user_mentions)}

def parse_users_columns(tweet):
    if 'user' in tweet:
        return parse_users_columns_native(tweet)
    elif 'actor' in tweet:
        return parse_users_columns_activity_streams(tweet)
    else:
        return {}

def parse_users_columns_native(tweet):
    created_at = tweet.get('created_at')
    obs_time = datetime.datetime.strptime(created_at, GNIP_DATETIME_FORMAT)
    user = tweet.get('user')
    created_at = user.get('created_at')
    created_time = datetime.datetime.strptime(created_at, GNIP_DATETIME_FORMAT)
    return {'id': user.get('id_str', ''),
            'friends_count': user.get('friends_count', -1),
            'num_follower': user.get('followers_count', -1),
            'displayname': user.get('name', ''),
            'preferredname': user.get('screen_name', ''),
            'obs_time': obs_time,
            'summary': user.get('description', ''),
            'status_count': user.get('statuses_count', -1),
            'languages': user.get('lang', ''),
            'listedcount': user.get('listed_count', -1),
            'created_time': created_time}

def parse_users_columns_activity_streams(tweet):
    obs_time = from_postedTime(tweet['postedTime'])

    user = tweet.get('actor')
    created_time = from_postedTime(user['postedTime'])
    id_str = extract_user_id(user['id'])
    lang = ','.join(user.get('languages', []))
    return {'id': id_str,
            'friends_count': user.get('friendsCount', -1),
            'num_follower': user.get('followersCount', -1),
            'displayname': user.get('displayName', ''),
            'preferredname': user.get('preferredUsername', ''),
            'obs_time': obs_time,
            'summary': user.get('summary', ''),
            'status_count': user.get('statusesCount', -1),
            'languages': lang,
            'listedcount': user.get('listedCount', -1),
            'created_time': created_time}


if __name__=="__main__":

    # default values
    # 
    # TODO
    #
    # We need to change tehese to Postgres...
    #
    HOST        = 'localhost'
    PORT        = '27017'
   
    # parse args
    parser = argparse.ArgumentParser(description='Insert JSON objects from stdin into PostgresDB')
    parser.add_argument('--host', 
                        type=str, 
                        default=HOST, 
                        help='Hostname of Postgres DB')
    parser.add_argument('--port', 
                        type=str, 
                        default=PORT, 
                        help='Port number of Postgres DB')
    parser.add_argument('--rules', 
                        type=str, 
                        default='', 
                        help='Path to file with Gnip rules, one per line')
    parser.add_argument('db', 
                        metavar='DB', 
                        type=str, 
                        help='Name of database to insert into')
    
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
    print 'Insert\tValueError\tNot Tweet\tLast tweet timestamp'

    # connect to db
    #
    # TODO
    #
    # Need to change this to whatever method we are using to connect with Postgres
    #
    #db = init(args.host, args.port, args.db)


    # Initialize counters
    inserts  = 0
    failures = 0
    valueerror = 0
    nottweet = 0

    # keep reading until there are no lines left
    line = next_line()
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
                print '{0}\t{1}\t{2}'.format(inserts, valueerror, nottweet)
            line = next_line()
            continue
        
        # Check if it's matching a Gnip rule we care about: 
        if rules:
            if not tweet_matches_rules(tweet, rules):
                line = next_line()
                continue

        # Is it a "native" Tweet (e.g., captured prior to Feb 28, 2012?
        # Or a tweet in Activity Streams format (Feb 28, 2012 and later...)
        if not (('text' in tweet) or ('body' in tweet)): 
            sys.stderr.write(line)
            sys.stderr.write('\n')
            nottweet += 1 
            if nottweet % 500 == 0:
                print '{0}\t{1}\t{2}'.format(inserts, valueerror, nottweet)
            line = next_line()
            continue


        # Parse the information we need for the tweets table
        tweet_data = parse_tweets_columns(tweet)

        # Parse out the data we need for the users table
        user_data = parse_users_columns(tweet)


        # If this was a mechanical retweet, get the retweeted tweet/user data
        retweeted_tweet_data = None
        retweeted_user_data = None
        if tweet_data.get('is_retweet'):
            if 'object' in tweet:
                retweeted_tweet = tweet['object']
                retweeted_tweet_data = parse_tweets_columns(retweeted_tweet)
                retweeted_user_data = parse_users_columns(retweeted_tweet)
            elif 'retweeted_status' in tweet:
                retweeted_tweet = tweet['retweeted_status']
                retweeted_tweet_data = parse_tweets_columns(retweeted_tweet)
                retweeted_user_data = parse_users_columns(retweeted_tweet)
            else:
                # This might have been a manual RT, in which case we don't have all the data
                pass 
                

        # Now insert these data into the database
        #
        # TODO
        #
        # Need a few lines of code here to insert the data... something like this...
        #
        if tweet_data:
            # INSERT INTO tweets ... 
            inserts += 1

        if user_data:
            # INSERT INTO users ...
            inserts += 1

        if retweeted_tweet_data:
            # INSERT INTO tweets ... 
            inserts += 1

        if retweeted_user_data:
            # INSERT INTO users ...
            inserts += 1

        if inserts % 50000 == 0:
            print '{0}\t{1}\t{2}'.format(inserts, valueerror, nottweet)

        # Finally, get a new line
        line = next_line()

    # Short summary to stdout
    print
    print 'Totals:'
    print 'Inserts\tFailures\tDate of last tweet inserted'
    print '{0}\t{1}\t{2}'.format(inserts, valueerror, nottweet)
