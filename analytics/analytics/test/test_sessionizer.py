from unittest import TestCase
from ..sessionizer import *
from pyspark import SparkContext
from datetime import *
import os

class TestSessionizer(TestCase):

    request1 = {'method': 'GET', 'url': 'http://test.com/url/'}

    session1 = Session(datetime(2016,02,28,hour=0),datetime(2016,02,28,hour=1), [request1])
    session2 = Session(datetime(2016, 02, 28, hour=1,minute=10), datetime(2016, 02, 28, hour=2), [request1])
    session3 = Session(datetime(2016, 02, 28, hour=3), datetime(2016, 02, 28, hour=4), [request1])

    @classmethod
    def setUpClass(cls):
        cls.sessionizer = Sessionizer(SparkContext(appName="Sessionizer"))
        cls.def_session_time_mins = timedelta(minutes=15)

    def test_merge_combiners_01(self):
        sessions1 = []
        sessions1.append(self.session1)
        sessions2 = []
        sessions2.append(self.session2)

        res = make_merge_combiners_func(self.def_session_time_mins)(sessions1,sessions2)

        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0].start == self.session1.start)
        self.assertTrue(res[0].end == self.session2.end)

    def test_merge_combiners_02(self):
        sessions1 = []
        sessions1.append(self.session1)
        sessions2 = []
        sessions2.append(self.session3)

        res = make_merge_combiners_func(self.def_session_time_mins)(sessions1, sessions2)

        self.assertTrue(len(res) == 2)
        self.assertTrue(res[0].start == self.session1.start)
        self.assertTrue(res[0].end == self.session1.end)
        self.assertTrue(res[1].start == self.session3.start)
        self.assertTrue(res[1].end == self.session3.end)

    def test_merge_combiners_exception_01(self):
        sessions1 = []
        sessions1.append(self.session1)
        sessions2 = []
        sessions2.append(self.session2)

        with self.assertRaises(SessionException) as context:
            res = make_merge_combiners_func(self.def_session_time_mins)(sessions2, sessions1)

    def test_merge_value_01(self):
        sessions = []
        sessions.append(self.session1)

        res = make_merge_value_func(self.def_session_time_mins)(sessions, self.session2)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0].start == self.session1.start)
        self.assertTrue(res[0].end == self.session2.end)

    def test_merge_value_02(self):
        sessions = []
        sessions.append(self.session1)

        res = make_merge_value_func(self.def_session_time_mins)(sessions, self.session3)
        self.assertTrue(len(res) == 2)
        self.assertTrue(res[0].start == self.session1.start)
        self.assertTrue(res[0].end == self.session1.end)
        self.assertTrue(res[1].start == self.session3.start)
        self.assertTrue(res[1].end == self.session3.end)


    def test_merge_value_exception_01(self):
        sessions = []
        sessions.append(self.session2)

        with self.assertRaises(SessionException) as context:
            res = make_merge_value_func(self.def_session_time_mins)(sessions, self.session1)


    def test_combiner(self):
        res = combiner(self.session1)

        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0].start == self.session1.start)
        self.assertTrue(res[0].end == self.session1.end)
        self.assertTrue(res[0].requests == self.session1.requests)

    def test_calc_sessions_from_file(self):
        filepath = 'data/log_sample.log'
        res = self.sessionizer.calc_sessions_from_file(filepath, 15)

        self.assertTrue(len(res) == 8)

    def test_calc_sessions_01(self):
        log = []
        log.append('2015-07-22T09:00:00.0Z balancer 1.1.1.1:10 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://test.com/url1 HTTP\"')
        log.append('2015-07-22T09:16:00.0Z balancer 1.1.1.1:10 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://test.com/url2 HTTP\"')
        res = self.sessionizer.calc_sessions_from_list(log, 15)

        self.assertTrue(len(res) == 1)
