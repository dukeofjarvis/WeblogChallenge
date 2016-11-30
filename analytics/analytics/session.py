from datetime import *
import iso8601
from urlparse import urlparse


class SessionException(Exception):
    pass


def parse_url_path(full_url):
    res = urlparse(full_url)
    return res.netloc + res.path

def session_from_entry(entry):
    elems = entry.split()
    session = {
        'id':hash(entry),
        'ip':elems[2].split(':')[0],
        'start':iso8601.parse_date(elems[0]),
        'end':iso8601.parse_date(elems[0]),
        'requests':[]}
    session['requests'].append(parse_url_path(elems[12]))
    return session

def session_combine(s1, s2):
    """
    Combine two web sessions and return a new session that spans the duration
    between the earliest start and latest end of both sessions


    :param s1: A web session
    :param s2: A web session
    :return: A combined web session
    """
    if not s1['ip'] == s2['ip']:
        raise SessionException('Combined sessions must have the same IP')
    new_start = s1['start'] if s1['start'] < s2['start'] else s2['start']
    new_end = s1['end'] if s1['end'] > s2['end'] else s2['end']
    s3 = {'ip':s1['ip'],
          'start':new_start,
          'end':new_end,
          'requests': s1['requests']+s2['requests']}
    return s3

def session_delta(s1, s2):
    """
    Calculate the time delta between two sessions.  Returns the positive delta between the end
    of a sessions and the start of the other session if they do not overlap. If the sessions
     overlap than 0 is returned

    :param s1:  A web session
    :param s2:  A web session
    :return: The time delta between the two sessions
    """
    delta1 = s1['start'] - s2['end']
    delta2 = s2['start'] - s1['end']
    delta3 = delta1 if delta1 > delta2 else delta2
    return delta3 if delta3 > timedelta(0) else timedelta(0)

def session_serialize(s):

    s['start'] = s['start'].isoformat()
    s['end'] = s['end'].isoformat()
    return s