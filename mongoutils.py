"""
Utility functions and constants
for working with MongoDB

Kevin Driscoll, 2012

"""
import pymongo

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

def from_postedTime(postedTime):
    """Convert date an ISO formatted strings to Python datetime objects
    """
    return datetime.datetime(int(postedTime[:4]),
                             int(postedTime[5:7]),
                             int(postedTime[8:10]),
                             int(postedTime[11:13]),
                             int(postedTime[14:16]),
                             int(postedTime[17:19]))

