#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Lengthen lots of shortURLs

Uses the Longurl.org API whenever possible
    http://longurl.org/api

Depends
    longurl https://bitbucket.org/mswbb/python-longurl/

TODO
    This works but is very slow. 
    Need to reduce redundant checks.

Kevin Driscoll, 2012, 2013

"""

from tweetutils import *
import codecs
import csv 
import httplib
import longurl
import json
import Queue
import requests
import sys 
import threading
import urllib
import urllib2

# Constants
USER_AGENT = u'shortURL lengthener/0.1 +http://kevindriscoll.info/'
DEBUG = True

# HTTP Error codes
HTTP_REDIRECT_CODES = [
    301, # Moved Permanently
    302, # Found, Moved temporarily
    303, # See other, Moved
    307,  # Temporary redirect
    '301', # Moved Permanently
    '302', # Found, Moved temporarily
    '303', # See other, Moved
    '307'  # Temporary redirect
]

# HTTP Timeout (in seconds)
# For more info on socket timeout:
# http://www.voidspace.org.uk/python/articles/urllib2.shtml#sockets-and-layers
HTTP_TIMEOUT = 13
HTTP_MAX_REDIRECTS = 13

# How big to let the queue grow before writing to disk
WRITE_QUEUE_SIZE = 200 

# Max number of threads to spawn while following URLs
THREAD_MAX = 101
THREAD_TIMEOUT = 11

class LazyHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, header, newurl):
        """On redirect, raise the HTTPError and die
        """
        return None

#
# Utility functions
#

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


#
# Communicating with longurl API
# http://longurl.org/api
#
# Realized that someone already wrote this up as a module in 2010! 
# Leaving this here for reference until I know for sure it's redundant
#

class Longurl():
    def __init__(self):
        self._session = requests.Session()
        self._session.headers['User-Agent'] = USER_AGENT
        self.known_services = []
        self.update_known_services() 
        self.cache = {}

    def update_known_services(self):
        endpoint = u'http://api.longurl.org/v2/services'
        r = self._session.get(endpoint,
                             params={'format': 'json'})
        if r.ok:
            known_services = r.json()
            self.known_services = known_services.keys()

    def lengthen(self, shorturl):
        # This lookup could take a very long time
        if shorturl in self.cache:
            return self.cache[shorturl]
        domain = urlparse.urlsplit(shorturl).netloc
        if domain in self.known_services:
            longurl = self._expand(shorturl)
            if 'rel-canonical' in longurl:
                lengthend = longurl['rel-canonical']
            else:
                lengthened = longurl['all-redirects'][-1]

            # Add URLs to cache
            self.cache[shorturl] = longurl 
            self.cache[lengthened] = lengthened
            for i in range(len(longurl['all-redirects'])-1):
                self.cache[longurl['all-redirects'][i]] = lengthened
        else:
            return 

    def _expand(self, shorturl):
        endpoint = u'http://api.longurl.org/v2/expand'
        params = {'format': 'json',
                    'url': shorturl,
                    'all-redirects': 1,
                    'content-type': 1,
                    'response-code': 1,
                    'title': 1,
                    'rel-canonical': 1,
                    'meta-keywords': 1,
                    'meta-description': 1}
        r = self._session.get(endpoint,
                            params=params)
        if r.ok:
            return r.json() 
        else:
            print r # TODO debug
            return None 


#
# Old lengthening function
#

def lengthen_url(u):
    """Return short_long dict of all URLs
    between u and its ultimate location"""
    
    # For description of error handling, see:
    # http://www.voidspace.org.uk/python/articles/urllib2.shtml#httperror

    # Create URL opener that doesn't auto follow redirs
    opener = urllib2.build_opener(LazyHTTPRedirectHandler)

    # Create list of URLs
    hops = [u]

    # Set nexturl to the first URL
    nexturl = u

    # Follow all redirects, adding URLs to hops 
    while nexturl and (len(hops) < HTTP_MAX_REDIRECTS):
        request = urllib2.Request(nexturl)
        try:
            r = opener.open(request, timeout=HTTP_TIMEOUT)
        except urllib2.HTTPError as err:
            if err.code in HTTP_REDIRECT_CODES:
                if u'location' in err.headers.keys():
                    loc = err.headers[u'location']
                    # Check for relative URL
                    if not loc[:4] == 'http':
                        nexturl = urllib.basejoin(err.geturl(), loc)
                    else:
                        nexturl = loc
                else:
                    nexturl = None
            else:
                nexturl = None
        except urllib2.URLError as err:
            # Server not found, etc.
            nexturl = None
        except ValueError:
            # Most likely an invalid URL
            nexturl = None
        except urllib2.httplib.BadStatusLine as err:
            # The server sent an unfamiliar status code 
            # Not caught by urllib2, see:
            # http://bugs.python.org/issue8823
            print err
            nexturl = None
        except urllib2.httplib.InvalidURL as err:
            # Usually happens when there is a colon
            # but no port number
            print err
            nexturl = None
        else:
            # Ultimate destination reached
            nexturl = None

        # Append the result to the hops list
        # None represents the end of the chain 
        hops.append(nexturl)

    # Construct dict from hops chain
    short_long = {}
    for i in range(len(hops) - 1):
        short_long[hops[i]] = hops[i + 1]
    
    # Return short_long dict
    return short_long


#
# Old file i/o functions
#

def write_short_long(short_long, output_fp):
    """Write short_long to output_fp as CSV
    """
    for shorturl, longurl in short_long.iteritems():
       
        short_utf8 = force_utf8(shorturl)
        long_utf8 = force_utf8(longurl)
        line = u'"{0}","{1}"\n'.format(
            short_utf8,
            long_utf8
        )
        try:
            output_fp.write(line.encode('utf-8'))
        except UnicodeDecodeError as err:
            print u'Caught exception trying to write to file:'
            print err
            try:
                print u'Trying to write without encoding...'
                output_fp.write(line)
            except ValueError as err:
                print u'Unknown exception trying to write to file.'
                print err
                print line 
            else:
                print u'It worked!'
        output_fp.flush()

def unicode_csv_reader(utf8_fp, **kwargs):
    # See: http://stackoverflow.com/questions/904041/reading-a-utf8-csv-file-with-python
    csv_reader = csv.reader(utf8_fp, **kwargs)
    while True:
        try:
            row = csv_reader.next()
        except UnicodeEncodeError as err:
            print err
            print "Skipping this row..."
        else:
            yield [unicode(cell, 'utf-8') for cell in row]


#
# Old thread worker functions
#

def lengthen_url_worker(url_queue, pairs_queue):
    """Lengthen URLs from url_queue
        Put output into pairs_queue
    """
    print "zug zug!",
    while True:
        try:
            url = url_queue.get(block=True, timeout=THREAD_TIMEOUT)
        except Queue.Empty: 
            print u'url_queue empty. Nothing left to lengthen. Returning to my cave.'
            break
        short_long = lengthen_url(url)
        # print url,
        # print len(short_long)
        pairs_queue.put(short_long)
        url_queue.task_done()
    
def row_reader_worker(url_tweetid_fp, unread_urls):
    """Read URLs from url_tweetid_fp
    Add to short_long_queue
    """
    csvr = unicode_csv_reader(url_tweetid_fp, delimiter=',')
    for row in csvr:
        unread_urls.put(row[0])
    print u'Done reading rows from input file.'

def short_long_writer_worker(short_long_queue, short_long_fp):
    """Pop pairs off of queue
        Write them to short_long_fp
    """
    while True:
        try:
            pair = short_long_queue.get(block=True, timeout=THREAD_TIMEOUT)
        except Queue.Empty:
            print "short_long_queue empty. Nothing left to write."
            break
        write_short_long(pair, short_long_fp)
        short_long_queue.task_done()

def lengthen_urls_parallel(url_tweetid_fp, short_long_fp):
    """Lengthen URLs from url_tweetid_fp
        in parallel using threads
        Write pairs to short_long_fp
        Returns number of URLs observed
    """
    unread_urls = Queue.Queue()
    short_long_pairs = Queue.Queue()

    threads = []
    threads.append(threading.Thread(target=row_reader_worker, args=(url_tweetid_fp, unread_urls)))
    for _ in range(THREAD_MAX):
        threads.append(threading.Thread(target=lengthen_url_worker, args=(unread_urls, short_long_pairs)))
    threads.append(threading.Thread(target=short_long_writer_worker, args=(short_long_pairs, short_long_fp)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()



#
# Functions for communicating with MongoDB
#



# 
# New functions rewritten on 9/29/2013
#


def shorturl_prep_worker(itertweets, shorturlq):
    """Take each tuple from the iterurls iterator.
        Add it to the shorturlq queue.
        Repeat.
    """
    for tweet in itertweets:
        tweet_id = tweet.get('_id')
        for u in tweet['twitter_entities']['urls']:
            if 'expanded_url' in u:
                shorturlq.put((tweet_id, u['expanded_url']))

            shorturlq.put((

    for (url_id, shorturl) in iterurls:
        shorturlq.put((url_id, shorturl))
    print u'Done reading URLs from source.'

def shorturl_lengthener_worker(short_queue, long_queue):
    """Lengthen URLs from shorturl_queue
        Put output into longurl_queue 
    """
    print "zug zug!",
    while True:
        try:
            (url_id, shorturl) = short_queue.get(block=True, timeout=THREAD_TIMEOUT)
        except Queue.Empty: 
            print u'url_queue empty. Nothing left to lengthen. Returning to my cave.'
            break
        short_long = lengthen_url(shorturl)
        long_queue.put((url_id, short_long))
        long_queue.task_done()
    
def short_long_writer_worker(longurl_queue, output_function):
    """Pop pairs off of queue
        Write them to short_long_fp
    """
    while True:
        try:
            (url_id, short_long) = longurl_queue.get(block=True, timeout=THREAD_TIMEOUT)
        except Queue.Empty:
            print "short_long_queue empty. Nothing left to write."
            break
        output_function(url_id, short_long)
        longurl_queue.task_done()

def lengthen_each(iter_short_urls, output_long_urls):
    """This is a manager function for a multithreaded process.
        It will start up multiple threads to lengthen URLs in parallel.
        iter_short_urls is an iterator that yields tuples: (url_id, short_urls)
    """

    shorturl_queue = Queue.Queue()
    longurl_queue = Queue.Queue()
    threads = []


    # Thread to read shortURLs out of source
    # and insert them into a queue
    threads.append(threading.Thread(target=shorturl_prep_worker, 
                                    args=(iter_short_urls, shorturl_queue)))

    # Thread(s) to read shortURLs out of the queue
    # Attempt to lengthen them
    # And insert them into the completed queue
    for _ in range(THREAD_MAX):
        threads.append(threading.Thread(target=shorturl_lengthener_worker, 
                                        args=(shorturl_queue, longurl_queue)))
    
    # Thread to read completed URLs out of the queue
    # And output them according to the output function
    threads.append(threading.Thread(target=short_long_writer_worker, 
                                    args=(short_long_pairs, short_long_fp)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()





if __name__=="__main__":

    host = "localhost"
    port = 9001
    database = "debate"
    input_collection = "oct2012"
    output_collection = "urls"

    observation_periods = [
        (datetime.datetime(2012, 10, 1),
        datetime.datetime(2012, 10, 4)), 
        (datetime.datetime(2012, 10, 15),
        datetime.datetime(2012, 10, 18)),
        (datetime.datetime(2012, 10, 21),
        datetime.datetime(2012, 10, 24)),
    ]

    # Connect to Mongo database instance
    mongo = pymongo.Connection(host=host, port=port)
    db = mongo[database]

    # This is a little funky
    # The idea is to have a simple function that we can pass to a worker 
    output_function = lambda obj: db[output_collection].save(obj)

    # TODO debuggin output 
    sys.stderr.write(u'Started at ')
    sys.stderr.write(datetime.datetime.now().isoformat())
    sys.stderr.write(u'\n')

    for start, end in observation_periods:
        query = {'postedTimeObj':
                    {'$gte': start,
                    '$lt': end}}
        projection = {'_id': True, 
                        'twitter_entities': True, 
                        'body': True, 
                        'postedTimeObj': True}
        # TODO limit() is for testing
        cursor = db.oct2012.find(query, projection).limit(50) 
        
        # Kick off the manager
        lengthen_each(cursor, db, output_collection)

    # TODO output the time for debugging
    sys.stderr.write(u'Finished at ')
    sys.stderr.write(datetime.datetime.now().isoformat())
    sys.stderr.write(u'\n')
