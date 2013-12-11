# -*- coding: ascii -*-
"""
Tweet comes in, CSV row goes out

Doesn't even try to support Unicode
This is just to grab quick exploratory subsets 
Not for srsbzns

Kevin Driscoll, 2013
"""

import csv
import fileinput
import json
import tweetutils


def build_row(tweet):
    rt = tweetutils.parse_retweet(tweet)
    hashtags = ','.join([t.get('text', '') for t in tweet.get('entities', {}).get('hashtags', [])])
    urls = ','.join([t.get('expanded_url', '') for t in tweet.get('entities', {}).get('urls', [])])
    mentions = ','.join([t.get('screen_name', '') for t in tweet.get('entities', {}).get('user_mentions', [])])
    mention_ids = ','.join([t.get('id_str', '') for t in tweet.get('entities', {}).get('user_mentions', [])])
    row = (tweet.get('id_str', ''),
            tweetutils.rfc2822_to_iso8601(tweet.get('created_at', '')),
            tweet.get('user', {}).get('screen_name', '').encode('ascii', errors="replace"),
            str(tweet.get('user', {}).get('id', '')),
            tweet.get('text', '').encode('ascii', errors="replace"),
            int(bool(rt)),
            int(rt.get('edited', False)),
            rt.get('retweeted_author_id_str', ''),
            rt.get('retweeted_author_screenname', '').encode('ascii', errors="replace"),
            int(bool(hashtags)),
            hashtags,
            int(bool(urls)),
            urls,
            int(bool(mentions)),
            mentions.encode('ascii', errors="replace"),
            mention_ids)
    return row


if __name__=="__main__":

    headings = ("tweet_id",
                    "created_at",
                    "author",
                    "author_id",
                    "text",
                    "rt",
                    "edited_rt",
                    "rt_author_id",
                    "rt_author_screenname",
                    "has_hashtag",
                    "hashtags",
                    "has_url",
                    "urls",
                    "has_mention",
                    "mentions",
                    "mention_ids")

    with open('output.csv', 'wb') as f:
        csvw = csv.writer(f, dialect="excel")
        csvw.writerow(headings)
        for l in fileinput.input():
            tweet = json.loads(l.strip())
            row = build_row(tweet)
            try:
                csvw.writerow(row)
            except UnicodeEncodeError:
                print tweet
