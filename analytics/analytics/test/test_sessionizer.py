from unittest import TestCase
from ..sessionizer import *
from pyspark import SparkContext
from datetime import *
import os

class TestSessionizer(TestCase):

    time1 = datetime(year=2016,month=02,day=28,minute=0)
    time2 = datetime(year=2016,month=02,day=28,minute=10)

    request1 = {'method': 'GET', 'url': 'http://test.com/url/'}
    request2 = {'method': 'POST', 'url': 'http://test.com/other/'}

    s1 = {'ip': '1.1.1.1',
          'start':time1,
          'end':time1,
          'requests':
          [request1]}
    s2 = {'ip': '1.1.1.1',
          'start':time2,
          'end':time2,
          'requests':
          [request2]}



    @classmethod
    def setUpClass(cls):
        cls.sessionizer = Sessionizer(SparkContext(appName="Sessionizer"))

    def test_merge_combiners_01(self):
        sessions1 = [self.s1]
        sessions2 = [self.s2]

        res = make_merge_combiners_func(timedelta(minutes=15))(sessions1,sessions2)

        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['start'] == self.s1['start'])
        self.assertTrue(res[0]['end']  == self.s2['end'])

    def test_merge_combiners_02(self):
        sessions1 = [self.s1]
        sessions2 = [self.s2]

        res = make_merge_combiners_func(timedelta(minutes=5))(sessions1, sessions2)

        self.assertTrue(len(res) == 2)
        self.assertTrue(res[0]['start'] == self.s1['start'])
        self.assertTrue(res[0]['end'] == self.s1['end'])
        self.assertTrue(res[1]['start'] == self.s2['start'])
        self.assertTrue(res[1]['end']  == self.s2['end'])

    def test_merge_combiners_03(self):
        sessions1 = [self.s1]
        sessions2 = [self.s2]

        res = make_merge_combiners_func(timedelta(minutes=10))(sessions1, sessions2)

        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['start'] == self.s1['start'])
        self.assertTrue(res[0]['end']  == self.s2['end'])

    def test_merge_combiners_exception_01(self):
        sessions1 = [self.s1]
        sessions2 = [self.s2]

        with self.assertRaises(SessionException) as context:
            res = make_merge_combiners_func(timedelta(minutes=10))(sessions2, sessions1)

    def test_merge_value_01(self):
        sessions = [self.s1]

        res = make_merge_value_func(timedelta(minutes=15))(sessions, self.s2)
        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['start'] == self.s1['start'])
        self.assertTrue(res[0]['end']  == self.s2['end'])

    def test_merge_value_02(self):
        sessions = [self.s1]

        res = make_merge_value_func(timedelta(minutes=5))(sessions, self.s2)

        self.assertTrue(len(res) == 2)
        self.assertTrue(res[0]['start'] == self.s1['start'])
        self.assertTrue(res[0]['end']  == self.s1['end'])
        self.assertTrue(res[1]['start'] == self.s2['start'])
        self.assertTrue(res[1]['end'] == self.s2['end'])

    def test_merge_value_exception_01(self):
        sessions = [self.s2]

        with self.assertRaises(SessionException) as context:
            res = make_merge_value_func(timedelta(minutes=5))(sessions, self.s1)

    def test_combiner(self):
        res = combiner(self.s1)

        self.assertTrue(len(res) == 1)
        self.assertTrue(res[0]['start'] == self.s1['start'])
        self.assertTrue(res[0]['end']  == self.s1['end'])


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
