from datetime import *


class SessionException(Exception):
    pass

class Session(object):

    def __init__(self, start, end):
        if not isinstance(start,datetime) or not isinstance(end, datetime):
            raise SessionException("Start and end times must be type datetime")
        if start > end:
            raise SessionException("Start must be before end")
        self._start = start
        self._end = end


    def serialize(self):
        d = {'start':self._start.isoformat(),'end':self._end.isoformat()}
        return d

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    def combine(self, other):
        """
        Combine two web sessions and return a new session that spans the duration
        between the earliest start and latest end of both sessions

        :param other: Another web session to combine
        :return: A combined session
        """
        new_start = self._start if self._start < other.start else other.start
        new_end = self._end if self._end > other.end else other.end
        return Session(new_start,new_end)

    def delta(self, other):
        """
        Calculate the time delta between two sessions.  Returns the delta between the end
        of this session and the start of the other session.

        If the sessions overlap than 0 is returned
        :param other: Another web session
        :return: The delta as a timedelta object
        """
        delta1 = self._start - other.end
        delta2 = other._start - self.end
        delta3 = delta1 if delta1 > delta2 else delta2
        return delta3 if delta3 > timedelta(0) else timedelta(0)