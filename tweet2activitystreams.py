# -*- coding: utf-8 -*-
"""
Tweet goes in; Gnip-style Activity Streams comes out

The purpose of this tool is to translate tweets from the
Streaming API into a format comparable to the tweets provided
by the Gnip PowerTrack Twitter Stream.

It started out as a single-use hackjob of Ryan Barrett's Twitter 
module. (Lo siento, Ryan!) Check that project out here:
https://github.com/snarfed/activitystreams-unofficial

Kevin Driscoll, 2013

"""

import datetime
import fileinput
import json
import re
import tweetutils
  
def rfc2822_to_iso8601(time_str):
    """Converts a timestamp string from RFC 2822 format to ISO 8601.

    Example RFC 2822 timestamp string generated by Twitter:
      'Wed May 23 06:01:13 +0000 2007'

    Resulting ISO 8610 timestamp string:
      '2007-05-23T06:01:13'
    """
    if not time_str:
      return None

    without_timezone = re.sub(' [+-][0-9]{4} ', ' ', time_str)
    dt = datetime.datetime.strptime(without_timezone, '%a %b %d %H:%M:%S %Y')
    return dt.isoformat()

def tag_uri(name):
    """Returns a tag URI string for the given domain and name.

    Example return value: 'tag:twitter.com,2012:snarfed_org/172417043893731329'

    Background on tag URIs: http://taguri.org/
    """
    return 'tag:twitter.com,%d:%s' % (datetime.datetime.now().year, name)

def user_url(username):
    """Returns the Twitter URL for a given user."""
    return 'http://twitter.com/%s' % (username)

def status_url(username, _id):
    """Returns the Twitter URL for a tweet from a given user with a given id."""
    uid = unicode(_id)
    return '%s/status/%s' % (user_url(username), _id)

def get_entities(tweet):
    entities = tweet.get('entities')
    if not entities:
        entities = {u'hashtags': [],
                    u'symbols': [],
                    u'urls': [],
                    u'user_mentions': []}
    return entities

def get_provider(tweet):
    """ Same for every tweet
    """
    return {u'displayName': u'Twitter',
            u'link': u'http://www.twitter.com',
            u'objectType': u'service'}

def build_generator(tweet):
    # yes, the source field has an embedded HTML link. bleh.
    # https://dev.twitter.com/docs/api/1.1/get/statuses/show/
    parsed = re.search('<a href="([^"]+)".*>(.+)</a>', tweet.get('source', ''))
    if parsed:
      url, name = parsed.groups()
      generator = {'displayName': name, 'url': url, 'link': url}
      return generator
    return None

def native_to_post(original):
    _id = unicode(original.get('id'))
    actor = native_user_to_actor(original.get('user'))
    postedTime = rfc2822_to_iso8601(original.get('created_at'))
    post = {u'_id': _id,
            u'actor': actor,
            u'body': original.get('text'),
            u'generator': build_generator(original),
            u'gnip': {}, 
            u'id': tag_uri(_id), 
            u'id_str': _id,
            u'link': status_url(actor.get('preferredUsername'), _id),
            u'object': native_to_object(original),
            u'objectType': 'activity',
            u'postedTime': postedTime,
            u'provider': get_provider(original),
            u'retweetCount': original.get('retweet_count'),
            u'twitter_entities': get_entities(original),
            u'verb': u'post'}
    return post

def native_to_share(rt):
    _id = unicode(rt.get('id'))
    actor = native_user_to_actor(rt.get('user'))
    postedTime = rfc2822_to_iso8601(rt.get('created_at'))
    _object = native_to_post(rt.get('retweeted_status'))
    share = {u'_id': _id,
                u'actor': actor,
                u'body': rt.get('text'),
                u'generator': build_generator(rt),
                u'gnip': {},
                u'id': tag_uri(_id),
                u'id_str': _id,
                u'link': status_url(actor.get('preferredUsername'), _id),
                u'object': _object,
                u'objectType': 'activity',
                u'postedTime': postedTime,
                u'provider': get_provider(rt),
                u'retweetCount': rt.get('retweet_count'),
                u'twitter_entities': get_entities(rt),
                u'verb': u'share'}
    return share

def native_to_object(tweet):
    screen_name = tweet.get('user', {}).get('screen_name')
    _id = unicode(tweet.get('id'))
    _object = {u'id': tag_uri(_id),
                u'id_str': _id,
                u'link': status_url(screen_name, _id),
                u'objectType': u'note',
                u'postedTime': rfc2822_to_iso8601(tweet.get('created_at')),
                u'summary': tweet.get('text')}
    return _object

def native_user_to_actor(user):
    _id = unicode(user.get('id'))
    actor = {u'displayName': user.get('name'),
                u'followersCount': user.get('followers_count'),
                u'friendsCount': user.get('friends_count'),
                u'id': tag_uri(_id),
                u'id_str': _id, 
                u'image': user.get('profile_image_url'),
                u'languages': user.get('lang'),
                u'link': user_url(user.get('screen_name')),
                u'links': [{u'href': user.get('url'), u'rel': u'me'}], 
                u'listedCount': user.get('listed_count'),
                u'location': {u'displayName': user.get('location'), u'objectType': u'place'},
                u'objectType': u'person',
                u'postedTime': rfc2822_to_iso8601(user.get('created_at')),
                u'preferredUsername': user.get('screen_name'), 
                u'statusesCount': user.get('statuses_count'),
                u'summary': user.get('description'),
                u'twitterTimeZone': user.get(u'time_zone'),
                u'utcOffset': user.get(u'utf_offset'), 
                u'verified': user.get(u'verified')}
    return actor

def native_to_gnip(native):
    if 'text' in native:
        if 'retweeted_status' in native:
            return native_to_share(native)
        else:
            return native_to_post(native)
    else:
        return {}


if __name__=="__main__":

    for line in fileinput.input():
        native = json.loads(line.strip())
        gnip = native_to_gnip(native)
        try:
            s = json.dumps(gnip, encoding="utf-16")
        except UnicodeDecodeError:
            s = json.dumps(gnip, encoding="utf-8")
        print s



