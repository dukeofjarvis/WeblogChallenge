from unittest import TestCase
from ..session import *


class TestSession_delta(TestCase):

    time1 = datetime(year=2016, month=02, day=28, minute=0)
    time2 = datetime(year=2016, month=02, day=28, minute=10)
    time3 = datetime(year=2016, month=02, day=28, minute=20)

    request1 = 'test.com/url/'
    request2 = 'test.com/other/'

    s1 = {'ip': '1.1.1.1',
          'start': time1,
          'end': time1,
          'requests':
              [request1]}
    s2 = {'ip': '1.1.1.1',
          'start': time2,
          'end': time2,
          'requests':
              [request2]}
    s3 = {'ip': '1.1.1.2',
          'start': time2,
          'end': time2,
          'requests':
              [request2]}
    s4 = {'ip': '1.1.1.1',
          'start': time1,
          'end': time3,
          'requests':
              [request2]}


    def test_session_delta(self):

        t = session_delta(self.s1,self.s2)
        self.assertTrue(t == timedelta(minutes=10))

    def test_session_delta_reverse_order(self):

        t = session_delta(self.s2,self.s1)
        self.assertTrue(t == timedelta(minutes=10))

    def test_session_delta_overlap(self):

        t = session_delta(self.s4,self.s2)
        self.assertTrue(t == timedelta(minutes=0))