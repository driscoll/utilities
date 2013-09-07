#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
tweetutils.py

Utility functions for dealing with tweets

Kevin Driscoll (c) 2011

"""
import codecs
import csv
import fileinput
import httplib
import json
import os
import re
import socket
import sys
import urllib2
# Gnip Activity Streams conforms to RFC3339 time formatting
# https://raw.github.com/tonyg/python-rfc3339/master/rfc3339.py
import rfc3339
from calendar import timegm
from time import time, strftime, strptime, gmtime 
from urlparse import urlsplit
from collections import defaultdict
from glob import glob
from operator import add 

# Make this longer if a lot of URLs aren't being resolved
LENGTHEN_TIMEOUT = 3

# String formatting 
GNIP_DATETIME_FORMAT = '%a %b %d %H:%M:%S +0000 %Y'
TWITTER_DATETIME_FORMAT = '%a, %d %b %Y %H:%M:%S +0000'
R6_DATETIME_FORMAT = '%m/%d/%y %H:%M'
CM_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# API calls
TWITTER_REST_API_URL = u'http://api.twitter.com/'
TWITTER_SEARCH_API_URL = u'http://search.twitter.com/search.json'

# Regular expressions
SOURCE_RE = re.compile(r'<a href="([^"]*?)" rel="nofollow">([^<]*?)</a>')
# INSANE URI matching regex from:
# http://daringfireball.net/2010/07/improved_regex_for_matching_urls
URL_RE = re.compile(r'(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?��....]))')

# YouTube URLs come in a few forms:
re_youtube_id = [
    re.compile(r'youtube.com/v/([A-Za-z0-9_-]{11})'), # youtube.com/v/{vidid}
    re.compile(r'youtube.com/vi/([A-Za-z0-9_-]{11})'), # youtube.com/vi/{vidid}
    re.compile(r'youtube.com/\?v=([A-Za-z0-9_-]{11})'), # youtube.com/?v={vidid}
    re.compile(r'youtube.com/\?vi=([A-Za-z0-9_-]{11})'), # youtube.com/?vi={vidid}
    re.compile(r'youtube.com/watch\?v=([A-Za-z0-9_-]{11})'), # youtube.com/watch?v={vidid}
    re.compile(r'youtube.com/watch\?vi=([A-Za-z0-9_-]{11})'), # youtube.com/watch?vi={vidid}
    re.compile(r'youtu.be/([A-Za-z0-9_-]{11})')     # youtu.be/{vidid}
]

# Regex to match Twitter ID substrings from Gnip object
re_tweet_id = re.compile(r':([0-9]*)$')
re_user_id = re.compile(r':([0-9]*)$')                

def extract_user_id(s):
    """ Return Twitter User ID found in s
        Return None if no matches found
    """
    m = re_user_id.search(s)
    if m:
        return m.group(1)
    else:
        return None

def extract_tweet_id(s):
    """ Return Twitter Tweet ID found in s
        Return None if no matches found
    """
    m = re_tweet_id.search(s)
    if m:
        return m.group(1)
    else:
        return None


def parse_youtube_id(url):
    """Parse a URL in search of a YouTube ID
        Returns str with YT ID or '' if not found
    """
    ytid = ''
    for regex in re_youtube_id:
        m = regex.search(url)
        if m:
            ytid = m.group(1)
            break
    return ytid

def force_utf8(s):
    """Return string s decoded with UTF-8
    """
    try:
        s_utf8 = u'{0}'.format(s)
    except UnicodeDecodeError as err:
        try:
            s_utf8 = u'{0}'.format(s.decode('utf-8', 'replace'))
        except UnicodeDecodeError as err:
            # Neither ascii nor utf8 could decode?
            print err
            print u'Tried but failed to fix it!'
            s_utf8 = None
    except:
        raise
    return s_utf8

def force_ascii(s):
    """Return string s encoded with ASCII
       Non-ASCII chars replaced with _
       Line breaks removed
    """
    if s:
        try:
            return s.encode('ascii','replace').replace('\n',' ').replace('\r',' ')
        except UnicodeDecodeError:
            decoded = force_utf8(s)
            return decoded.encode('ascii','replace').replace('\n',' ').replace('\r',' ')
    else:
        return None

def dumb_csv_row(seq):
    """Write seq into a CSV row
    """
    return u'"{0}"'.format(u'","'.join(seq))

def error_log(s):
    timestamp = unicode(time())
    try:
        print ': '.join([timestamp, s])
    except:
        print '(Error trying to print error. Sorry!)'

def extract_source(source):
    """Return (name, url) from source field
    """
    m = SOURCE_RE.match(source)
    if m:
        url = m.group(1)
        name = m.group(2)
    else:
        url = ''
        name = source
    return name, url

def extract_domain(url):
    """Try to extract domain and TLD from url
        Returns empty str if not found
    """
    if url:
        url = clean_url(url)
        urlparts = urlsplit(url)
        try:
            domain = '.'.join(urlparts.hostname.rsplit('.',2)[-2:]) 
        except AttributeError:
            domain = None
        return domain
    else:
        return None

def clean_url(url):
    """Return lowercased with http:// added if neccessary
    """
    # Sometimes urls don't include http(s)
    if not (url[0:4] == 'http'):
        url = 'http://'+url
    return url.strip()

def lengthen(shorturl):
    """ Lengthen a shortened URL
        shorturl : string containing a shortened URL
        Returns longurl as unicode string
    """
    longurl = shorturl
    # Try to resolve the URL over the network
    # TODO this takes a long time, must be a better way?
    # Looks like some good ideas here:
    # http://stackoverflow.com/questions/316866/ping-a-site-in-python
    try:
        u = urllib2.urlopen(shorturl, timeout=LENGTHEN_TIMEOUT)
        encoding = 'utf-8'
        if 'content-type' in u.headers.keys():
            if (u.headers['content-type'].find('charset') > -1):
                encoding = u.headers['content-type'].split('charset=')[-1].lower()
        try:
            longurl = unicode(u.geturl(), encoding, 'replace')
        except TypeError:
            # This happens if the url is
            # already Unicode
            longurl = u.geturl()
        except LookupError:
            # Unknown encoding
            longurl = u'Unknown encoding.'
    except UnicodeDecodeError, e:
        error_log(u'UnicodeDecodeError trying to lengthen this URL: {0}'.format(shorturl))
        error_log(unicode(e))
        longurl = unicode(e)
    except UnicodeEncodeError, e:
        error_log(u'UnicodeEncodeError trying to lengthen this URL: {0}'.format(shorturl))
        error_log(unicode(e))
        longurl = unicode(e)
    except urllib2.URLError, e:
        error_log(u'urllib2.URLError trying to lengthen this URL: {0}'.format(shorturl))
        error_log('urllib2.URLError trying to lengthen this URL: {0}'.format(shorturl))
        error_log(unicode(e))
        longurl = unicode(e)
    except socket.timeout:
        # Found out about this weird bug the hard way
        # http://heyman.info/2010/apr/22/python-urllib2-timeout-issue/
        error_log(u'socket.timeout error trying to lengthen this URL: {0}'.format(shorturl))
        error_log(u'Socket timed out.')
        longurl = u'Socket timed out.'
    except httplib.InvalidURL:
        # This happened once with:
        # http://www.http.com//motherboard.tv/2011/11/18/who-smashed-the-laptops-from-occupy-wall-street-inside-the-nypd-s-lost-and-found
        error_log(u'httplib.InvalidURL trying to lengthen this URL: {0}'.format(shorturl))
        longurl = u'Invalid URL.'
    except httplib.BadStatusLine:
        # This happened once with:
        # http://www.feministas.org/spip.php?article230
        error_log(u'Error trying to lengthen this URL: {0}'.format(shorturl))
        longurl = u'BadStatusLine.'
    return longurl

def epoch_to_timestamp(epoch, timestamp_format=GNIP_DATETIME_FORMAT):
    """Convert string timestamp to sec since epoch
    using date_format to decode the string
    default format stored in GNIP_DATETIME_FORMAT const
    """
    try:
        timestamp = strftime(timestamp_format, gmtime(epoch))
    except ValueError:
        # timestamp didn't match the timestamp_format
        timestamp = u''
    return timestamp 

def timestamp_to_epoch(timestamp, timestamp_format=GNIP_DATETIME_FORMAT):
    """Convert string timestamp to sec since epoch
    using date_format to decode the string
    default format stored in GNIP_DATETIME_FORMAT const
    """
    try:
        epoch = timegm(strptime(timestamp, timestamp_format)) 
    except ValueError:
        # timestamp didn't match the timestamp_format
        epoch = u''
    return epoch

def itertweets(gnipfn):
    """Iterator yields valid tweet objects found in gnipfn 
        gnipfn : (str) path to input file (gnip tweets)
    """
    # Try to open gnipfn
    try:
        gnipfp = open(gnipfn, 'rb')
    except:
        print 'Could not access {0}'.format(gnipfn) 
    else:
        for raw in gnipfp:
            # Strip out extra space, newlines
            line = raw.strip()
            # Is it a blank line?
            if not line:
                continue

            # Is it a valid JSON object?
            try:
                tweet = json.loads(line)
            except ValueError as e:
                print u'Caught exception: ',
                print e
                print u'With this data: ',
                print line
                continue

            # Is it a Twitter object? (Specifically, does it have a 'created_at' key?)
            if (u'created_at' in tweet.keys()):
                yield(tweet)
                
            # Or is it a Gnip Activity Streams object?
            if (u'gnip' in tweet.keys()):
                yield(tweet)

            # Otherwise, ditch it
            continue

def die(s=None):
    if s:
        print s.encode('utf-8','replace')
        print
    sys.exit(1)
