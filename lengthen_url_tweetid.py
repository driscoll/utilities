#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Try to lengthen all short URLs
from output of extract_urls_from_raw.py

Input files should be:

url_tweetid_YYYY-MM-DD.csv

Input data should be:

"url","tweetid"
"http://some.url/foo","012345678901234567"

Appends new short, long pairs to:

short_long_YYYY-MM-DD.csv

"""

from glob import glob
from operator import add 
from tweetutils import *
import codecs
import httplib
import urllib
import urllib2
import csv 
import json
import sys 
import threading
import Queue

# Constants
USER_AGENT = u'OWS Tweets/0.1 +http://civicpaths.uscannenberg.org/'
DEBUG = True

# Filename formats
URL_TWEETID_FORMAT = u'url_tweetid_%Y-%m-%d.csv'
SHORT_LONG_FORMAT = u'short_long_%Y-%m-%d.csv'

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

# Time format
DIGITAL_CLOCK_FORMAT = u'%H:%M:%S'

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

def construct_request_longurlplease(queue):
    """Return api call for longurlplease.com
    from URLs listed in queue
    """
    url = u'http://longurlplease.appspot.com/api/v1.1'
    values = [urllib.urlencode({'q':u.encode('utf-8')}) for u in queue] 
    data = '&'.join(values)
    req = '?'.join([url, data])
    return req

def construct_request_longurl(shorturl):
    """Return api call for longurl.org
    for one url
    """
    url = u'http://api.longurl.org/v2/expand'
    values = [
        urllib.urlencode({'url':shorturl.encode('utf-8')}),
        urllib.urlencode({'format':'json'}),
        urllib.urlencode({'all-redirects':1})
    ]
    data = '&'.join(values)
    req = '?'.join([url, data])
    request = urllib2.Request(req)
    request.add_header('User-Agent', USER_AGENT)
    return request

def lengthen_url_list(queue):
    """ Returns dict of short:long pairs
        TODO this could be refactored and recursive
    """
    opener = urllib2.build_opener()
    req = construct_request(queue)
    try:
        response = opener.open(req, timeout=HTTP_TIMEOUT)
    except urllib2.HTTPError, err:
        print err
        line = u'\n'.join(queue)
        print line.encode('utf-8')
        return {}

    short_long = json.loads(response.read())
    queue = [longurl for longurl in short_long.itervalues() if longurl]

    # Repeat until you hit the end of all URLs
    # TODO are there any URLs that never end?
    while (len(queue) > 0):
        req = construct_request(queue)
        try:
            response = opener.open(req, timeout=HTTP_TIMEOUT)
        except urllib2.HTTPError, err:
            line = u'\n'.join(queue)
            print line.encode('utf-8')
            break
        # Test for exception here?
        new_pairs = json.loads(response.read())
        # Add new pairs to short_long
        short_long.update(new_pairs)
        # Reload the queue
        queue = [longurl for longurl in new_pairs.itervalues() if longurl]
    # Return short:long pairs 
    return short_long

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




def usage():
    print u'Oof. You forgot to specific input files(s).'
    print u'Usage:'
    print u'$ python expand_shorturl_longurl.py url_tweetid_201*.csv'
    sys.exit(1)

def lengthen_urls_sequential(url_tweetid_fp, short_long_fp):
    """Lengthen URLs from url_tweetid_fp
        Write pairs to short_long_fp
        Returns number of URLs observed
    """
    url_count = 0
    row_count = 0 
    pairs = {}
    for row in csv.reader(url_tweetid_fp, delimiter=','):
        url = row[0]
        short_long = lengthen_url(url)
        pairs.update(short_long)
        row_count += 1
        url_count += len(short_long)
        if (url_count % WRITE_QUEUE_SIZE) == 0:
            if DEBUG:
                print row_count,
                print u'\t',
                print url_count,
                print u'\t',
                print url
            write_short_long(pairs, short_long_fp)
            pairs = {}
    write_short_long(pairs, short_long_fp)
    return url_count

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




def get_short_long_fp(url_tweetid_fn):
    """Return file obj for output based on format of url_tweetid_fn
    """
    try:
        short_long_fn = strftime(SHORT_LONG_FORMAT, strptime(url_tweetid_fn, URL_TWEETID_FORMAT))
        short_long_fp = codecs.open(short_long_fn, 'ab', 'utf-8')
    except ValueError as err:
        if DEBUG:
            print err
            print "Expected format: ",
            print URL_TWEETID_FORMAT
            print "Found: ",
            print url_tweetid_fn
            print "Skipping this input file."
        short_long_fp = None
    except IOError as err:
        if DEBUG:
            print err
            print "Skipping this input file."
        short_long_fp = None
    return short_long_fp

def get_url_tweetid_fp(url_tweetid_fn):
    """Return file obj containing url_tweetid CSV
    """
    try:
        # csv module doesn't support UTF-8
        # url_tweetid_fp = codecs.open(url_tweetid_fn, 'rb', 'utf-8')
        # open the traditional way 
        url_tweetid_fp = open(url_tweetid_fn, 'rb')
    except IOError as err:
        if DEBUG:
            print err
            print "Skipping this input file."
        url_tweetid_fp = None
    return url_tweetid_fp

def summary(fn, start, end, count=-1):
    """Return nicely formatted summary string
    """
    # Print out summary
    summary = [u'Started at {0}'.format(strftime(DIGITAL_CLOCK_FORMAT, start))]
    if count > -1:
        summary.append(u'Followed {0} links'.format(count))
    summary.append(u'Finished at {0}'.format(strftime(DIGITAL_CLOCK_FORMAT, end)))
    return u'\n'.join(summary)



if __name__=="__main__":

    if (len(sys.argv) < 2):
        usage()
   
    input_files = reduce(add, map(glob, sys.argv[1:]))

    for fn in input_files:

        print 'Lengthening URLs from: {0} ...'.format(fn)

        url_tweetid_fp = get_url_tweetid_fp(fn)
        if not url_tweetid_fp:
            continue

        short_long_fp = get_short_long_fp(fn) 
        if not short_long_fp:
            continue 

        start_time = gmtime(time()-28800)
        # url_count = lengthen_urls_sequential(url_tweetid_fp, short_long_fp)
        lengthen_urls_parallel(url_tweetid_fp, short_long_fp)
        end_time = gmtime(time()-28800)
        # print summary(fn, start_time, end_time, url_count)
        print summary(fn, start_time, end_time)

