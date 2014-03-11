# -*- coding: utf-8 -*-
"""
Zero Notifications

Turn device notifications OFF for every user that you follow.

Kevin Driscoll, 2014
"""

from requests_oauthlib import OAuth1, OAuth1Session
from urlparse import parse_qs
import argparse
import requests
import webbrowser

def get_session(client_key,
                        client_secret,
                        resource_owner_key=None,
                        resource_owner_secret=None):
    """Return an authenticated OAuth1Session object
        Followed the examples here:
        https://requests-oauthlib.readthedocs.org/en/latest/oauth1_workflow.html
    """
    if not resource_owner_key or not resource_owner_secret:
        resource_owner_key, resource_owner_secret = authorize(client_key, client_secret)
    return OAuth1Session(client_key,
                            client_secret=client_secret,
                            resource_owner_key=resource_owner_key,
                            resource_owner_secret=resource_owner_secret)

def authorize(consumer_key, consumer_secret):

    # Uses oauth lib by Requests - https://github.com/requests/requests-oauthlib/
    # Current release of requests_oauthlib (0.2.0) does not automatically encode the
    # parameters in the call to OAuth1 to utf-8.
    # Encodes explicitly all the strings that go into the OAuth1 constructor to utf-8
    # to circumvent the error 'ValueError: Only unicode objects are escapable.'
    # The code does not currently check if the user has already authentcated our app.
    # It redirects the user to the browser everytime to authenticate.

    # OAuth endpoints
    request_token_url = u"https://api.twitter.com/oauth/request_token"
    authorize_url = u"https://api.twitter.com/oauth/authorize?oauth_token="
    access_token_url = u"https://api.twitter.com/oauth/access_token"

    # Obtain the request tokens.
    # These are used to redirect the user to the authorization URL to get the verifier PIN
    oauth = OAuth1(consumer_key, client_secret=consumer_secret)
    r = requests.post(url=request_token_url, auth=oauth)

    credentials = parse_qs(r.content)
    request_token = credentials['oauth_token'][0]
    request_token = unicode(request_token, 'utf-8')
    request_secret = credentials['oauth_token_secret'][0]
    request_secret = unicode(request_secret, 'utf-8')

    # Prompt the user to verify the app at the authorization URL and get the verifier PIN
    authorize_url = authorize_url + request_token
    print "Redirecting you to the browser to authorize...", authorize_url
    webbrowser.open(authorize_url)
    verifier = raw_input('Please enter your PIN : ')
    verifier = unicode(verifier, 'utf-8')

    # Once the user enters the PIN, we store the users access token and secret.
    # This is used for further operations by this user.
    oauth = OAuth1(consumer_key,
                    client_secret=consumer_secret,
                    resource_owner_key=request_token,
                    resource_owner_secret=request_secret,
                    verifier=verifier)
    r = requests.post(url=access_token_url, auth=oauth)
    credentials = parse_qs(r.content)
    access_token = credentials.get('oauth_token')[0]
    access_token = unicode(access_token, 'utf-8')
    access_secret = credentials.get('oauth_token_secret')[0]
    access_secret = unicode(access_secret, 'utf-8')
    return access_token, access_secret

def get_user_details(twitter):
    """Retrieve account information about the authenticated user

        twitter: OAuth1Session object authenticated already
    """ 
    url = "https://api.twitter.com/1.1/account/verify_credentials.json"
    params = {"include_entities": False, "skip_status": True}
    response = twitter.get(url=url, params=params)
    return response.json()

def iter_friend_ids(twitter, user_id):
    url = "https://api.twitter.com/1.1/friends/ids.json"
    next_cursor = -1
    while next_cursor:
        params = {"user_id": user_id,
                    "next_cursor": next_cursor,
                    "stringify_ids": True,
                    "count": 5000}
        response = twitter.get(url=url, params=params)
        data = response.json()
        for uid in data.get("ids", []):
            yield uid
        next_cursor = data.get("next_cursor")

def no_notifications(twitter, uid):
    url = "https://api.twitter.com/1.1/friendships/update.json"
    params = {"user_id": uid, "device": False}
    response = twitter.post(url=url, params=params)
    return response.json()
 

if __name__=="__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--clientkey',
                        type=str,
                        default=u"",
                        help="Consumer Key")
    parser.add_argument('--clientsecret',
                        type=str,
                        default=u"",
                        help="Consumer Secret")
    parser.add_argument('--resourcekey',
                        type=str,
                        default=u"",
                        help="Access Token")
    parser.add_argument('--resourcesecret',
                        type=str,
                        default=u"",
                        help="Access Token Secret")
    args = parser.parse_args()

    consumer_key = args.clientkey
    consumer_secret = args.clientsecret
    access_token = args.resourcekey
    access_token_secret = args.resourcesecret


    sesh = get_session(consumer_key,
                            consumer_secret,
                            access_token,
                            access_token_secret)
    
    user = get_user_details(sesh)
    print "Zeroing notifications for",
    print user.get("screen_name")

    user_id = user["id_str"]
    failures = []
    for n, uid in enumerate(iter_friend_ids(sesh, user_id)):
        response = no_notifications(sesh, uid)
        relationship = response["relationship"]
        if relationship["source"]["notifications_enabled"]:
            failures.append(relationship)

    print "Notifications turned off for", (n-len(failures)), "friends."
    print "Failed to turn off notifications for", len(failures), "friends."
