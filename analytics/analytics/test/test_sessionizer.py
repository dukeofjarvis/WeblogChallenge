from unittest import TestCase
from ..sessionizer import *
from pyspark import SparkContext
from datetime import *

class TestSessionizer(TestCase):

    time1 = datetime(year=2016,month=02,day=28,minute=0)
    time2 = datetime(year=2016,month=02,day=28,minute=10)
    time3 = datetime(year=2016, month=02, day=28, minute=20)

    request1 = 'test.com/url/'
    request2 = 'test.com/other/'

    s1 = {
        'id':'s1',
        'ip': '1.1.1.1',
        'start':time1,
        'end':time1,
        'requests':
        [request1]}
    s2 = {
        'id':'s2',
        'ip': '1.1.1.1',
        'start':time2,
        'end':time2,
        'requests':
        [request2]}
    s3 = {
        'id':'s3',
        'ip': '1.1.1.2',
        'start': time2,
        'end': time3,
        'requests':
        [request2, request1]}
    s4 = {
        'id':'s4',
        'ip': '1.1.1.1',
        'start': time1,
        'end': time3,
        'requests':
        [request2]}
    s5 = {
        'id':'s5',
        'ip': '1.1.1.3',
        'start': time1,
        'end': time3,
        'requests':
        [request1, request1]}

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext(appName="Sessionizer")
        cls.sessionizer = Sessionizer(cls.sc)

    def test_calc_sessions_from_file(self):
        filepath = 'data/log_sample.log'
        res = self.sessionizer.calc_sessions_from_file(filepath, 15)
        res_json = self.sessionizer.sessions_to_json(res)

        self.assertTrue(len(res_json) == 8)

    def test_calc_sessions_same_ips_diff_sessions(self):
        log = []
        log.append('2015-07-22T09:00:00.0Z balancer 1.1.1.1:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url1 HTTP\"')
        log.append('2015-07-22T09:16:00.0Z balancer 1.1.1.1:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url2 HTTP\"')
        res = self.sessionizer.calc_sessions_from_list(log, 15)
        res_json = self.sessionizer.sessions_to_json(res)

        self.assertTrue(len(res_json) == 2)
        self.assertTrue(res_json[0]['ip'] == '1.1.1.1')
        self.assertTrue(res_json[0]['start'] == '2015-07-22T09:00:00+00:00')
        self.assertTrue(res_json[0]['end'] == '2015-07-22T09:00:00+00:00')
        self.assertTrue(len(res_json[0]['requests']) == 1)
        self.assertTrue(res_json[0]['requests'][0] == 'test.com/url1')
        self.assertTrue(res_json[1]['ip'] == '1.1.1.1')
        self.assertTrue(res_json[1]['start'] == '2015-07-22T09:16:00+00:00')
        self.assertTrue(res_json[1]['end'] == '2015-07-22T09:16:00+00:00')
        self.assertTrue(len(res_json[1]['requests']) == 1)
        self.assertTrue(res_json[1]['requests'][0] == 'test.com/url2')


    def test_calc_sessions_same_ips_same_session(self):
        log = []
        log.append(
            '2015-07-22T09:00:00.0Z balancer 1.1.1.1:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url1 HTTP\"')
        log.append(
            '2015-07-22T09:16:00.0Z balancer 1.1.1.1:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url2 HTTP\"')
        res = self.sessionizer.calc_sessions_from_list(log, 20)
        res_json = self.sessionizer.sessions_to_json(res)

        self.assertTrue(len(res_json) == 1)
        self.assertTrue(res_json[0]['ip'] == '1.1.1.1')
        self.assertTrue(res_json[0]['start'] == '2015-07-22T09:00:00+00:00')
        self.assertTrue(res_json[0]['end'] == '2015-07-22T09:16:00+00:00')
        self.assertTrue(len(res_json[0]['requests']) == 2)
        self.assertTrue('test.com/url1' in res_json[0]['requests'])
        self.assertTrue('test.com/url2' in res_json[0]['requests'])

    def test_calc_sessions_unordered_log_times(self):
        log = []
        log.append(
            '2015-07-22T09:16:00.0Z balancer 1.1.1.1:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url2 HTTP\"')
        log.append(
            '2015-07-22T09:00:00.0Z balancer 1.1.1.1:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url1 HTTP\"')
        res = self.sessionizer.calc_sessions_from_list(log, 20)
        res_json = self.sessionizer.sessions_to_json(res)

        self.assertTrue(len(res_json) == 1)
        self.assertTrue(res_json[0]['ip'] == '1.1.1.1')
        self.assertTrue(res_json[0]['start'] == '2015-07-22T09:00:00+00:00')
        self.assertTrue(res_json[0]['end'] == '2015-07-22T09:16:00+00:00')
        self.assertTrue(len(res_json[0]['requests']) == 2)
        self.assertTrue('test.com/url1' in res_json[0]['requests'])
        self.assertTrue('test.com/url2' in res_json[0]['requests'])

    def test_calc_sessions_diff_ips(self):
        log = []
        log.append('2015-07-22T09:00:00.0Z balancer 1.1.1.1:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url1 HTTP\"')
        log.append('2015-07-22T09:16:00.0Z balancer 1.1.1.2:10 3 4 5 6 7 8 9 10 \"GET https://test.com/url2 HTTP\"')
        res = self.sessionizer.calc_sessions_from_list(log, 15)
        res_json = self.sessionizer.sessions_to_json(res)

        self.assertTrue(len(res_json) == 2)
        self.assertTrue(res_json[0]['ip'] == '1.1.1.1')
        self.assertTrue(res_json[0]['start'] == '2015-07-22T09:00:00+00:00')
        self.assertTrue(res_json[0]['end'] == '2015-07-22T09:00:00+00:00')
        self.assertTrue(len(res_json[0]['requests']) == 1)
        self.assertTrue(res_json[0]['requests'][0] == 'test.com/url1')
        self.assertTrue(res_json[1]['ip'] == '1.1.1.2')
        self.assertTrue(res_json[1]['start'] == '2015-07-22T09:16:00+00:00')
        self.assertTrue(res_json[1]['end'] == '2015-07-22T09:16:00+00:00')
        self.assertTrue(len(res_json[1]['requests']) == 1)
        self.assertTrue(res_json[1]['requests'][0] == 'test.com/url2')

    def test_calc_average_session_time(self):
        sessions = [self.s1,self.s4, self.s3]
        sessions_rdd = self.sc.parallelize(sessions)
        avg_time = self.sessionizer.average_session_time(sessions_rdd)

        self.assertTrue(avg_time == 10 * 60)


    def test_unique_visits_per_session(self):
        sessions = [self.s2,self.s3, self.s5]
        sessions_rdd = self.sc.parallelize(sessions)
        unique_visits = self.sessionizer.unique_visits_per_session(sessions_rdd)

        self.assertTrue(unique_visits[self.s2['id']] == 1)
        self.assertTrue(unique_visits[self.s3['id']] == 2)
        self.assertTrue(unique_visits[self.s5['id']] == 1)


    def test_find_engaged_users(self):
        sessions = [self.s2,self.s3, self.s5]
        sessions_rdd = self.sc.parallelize(sessions)
        engaged = self.sessionizer.find_engaged_users(sessions_rdd)

        self.assertTrue(engaged[0]['id'] == self.s5['id'])
        self.assertTrue(engaged[1]['id'] == self.s3['id'])
        self.assertTrue(engaged[2]['id'] == self.s2['id'])