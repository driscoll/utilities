"""
Utility functions and constants
for working with MongoDB

Kevin Driscoll, 2012

"""
import pymongo
import rfc3339

def tweet_matches_rules(thistweet, somerules):
    """ Returns true if thistweet matched one 
        of the Gnip rules in somerules
    """
    match_found = False
    for match in thistweet['gnip']['matching_rules']:
        if match['value'] in somerules:
            match_found = True
            break
    return match_found

def convert_date(tweetdate):
    """ tweetdate is string representing the date in 
        Gnip's Activity Streams format
        Return datetime obj for insert into mongodb
    """
    return rfc3339.parse_datetime(tweetdate)


