from unittest import TestCase
from ..session import *


class TestCombine_sessions(TestCase):

    time1 = datetime(year=2016,month=02,day=28,minute=0)
    time2 = datetime(year=2016,month=02,day=28,minute=10)
    time3 = datetime(year=2016,month=02,day=28,minute=20)

    request1 = 'test.com/url/'
    request2 = 'test.com/other/'

    s1 = {'id':'s1',
        'ip': '1.1.1.1',
        'start':time1,
        'end':time1,
        'requests':
        [request1]}
    s2 = {'id':'s2',
        'ip': '1.1.1.1',
        'start':time2,
        'end':time2,
        'requests':
        [request2]}
    s3 = {'id':'s3',
        'ip': '1.1.1.2',
        'start':time2,
        'end':time2,
        'requests':
        [request2]}
    s4 = {'id':'s4',
        'ip': '1.1.1.1',
        'start':time1,
        'end':time3,
        'requests':
        [request2]}

    def test_combine_different_requests(self):
        s = session_combine(self.s1, self.s2)

        self.assertTrue(self.request1 in s['requests'])
        self.assertTrue(self.request2 in s['requests'])
        self.assertTrue(s['start'] == self.time1)
        self.assertTrue(s['end'] == self.time2)

    def test_combine_session_within_session(self):
        s = session_combine(self.s2, self.s4)

        self.assertTrue(self.request2 in s['requests'])
        self.assertTrue(s['start'] == self.time1)
        self.assertTrue(s['end'] == self.time3)

    def test_combine_sessions_same_requests(self):
        s = session_combine(self.s2, self.s4)

        self.assertTrue(self.request2 in s['requests'])
        self.assertTrue(len(s['requests']) == 2)

    def test_combine_different_ips_exception(self):

        with self.assertRaises(SessionException) as context:
            s = session_combine(self.s1, self.s3)
