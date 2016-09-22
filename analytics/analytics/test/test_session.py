from unittest import TestCase
from ..session import *
from datetime import datetime
import json

class TestSession(TestCase):

    start1 = datetime(2016, 02, 28, 2, 0, 1)
    start2 = datetime(2016, 02, 28, 2, 30, 1)

    end1 = datetime(2016, 02, 28, 2, 0, 1)
    end2 = datetime(2016, 02, 28, 3, 0, 1)

    request1_str = 'GET http://test.com/url/'
    request2_str = 'POST http://test.com/other/'

    request1 = {'method':'GET','url':'http://test.com/url/'}
    request2 = {'method':'POST','url':'http://test.com/other/'}

    entry = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"

    def test_session_from_entry(self):
        s = Session.from_log_entry(self.entry)
        self.assertTrue(s.start == iso8601.parse_date('2015-07-22T09:00:28.019143Z'))
        self.assertTrue(s.end == iso8601.parse_date('2015-07-22T09:00:28.019143Z'))
        self.assertTrue(s.requests[0]['method'] == 'GET')
        self.assertTrue(s.requests[0]['url'] == 'https://paytm.com:443/shop')

    def test_session_success_01(self):
        s = Session(self.start1, self.end1, [self.request1])
        self.assertTrue(s.start == self.start1)
        self.assertTrue(s.end == self.end1)
        self.assertTrue(s.requests[0]['method'] == 'GET')
        self.assertTrue(s.requests[0]['url'] == 'http://test.com/url/')

    def test_session_success_02(self):
        s = Session(self.start1, self.end2, [self.request1])
        self.assertTrue(s.start == self.start1)
        self.assertTrue(s.end == self.end2)

    def test_session_exception_01(self):
        with self.assertRaises(SessionException) as context:
            s = Session(self.end2, self.start1, [self.request1])

    def test_combine_01(self):
        s1 = Session(self.start1, self.end1, [self.request1])
        s2 = Session(self.start2, self.end2, [self.request2])
        s3 = s1.combine(s2)
        self.assertTrue(s3.start == s1.start)
        self.assertTrue(s3.end == s2.end)

        s4 = s2.combine(s1)
        self.assertTrue(s4.start == s1.start)
        self.assertTrue(s4.end == s2.end)

    def test_delta(self):
        s1 = Session(self.start1, self.end1, [self.request1])
        s2 = Session(self.start2, self.end2, [self.request1])

        d1 = s1.delta(s2)
        self.assertTrue(d1 == timedelta(minutes=30))

    def test_session_serialize(self):
        s = Session(self.start1, self.end2, [self.request1])
        json_ses = json.dumps(s.serialize())
        sdict = json.loads(json_ses)
        self.assertTrue(sdict['start'] == self.start1.isoformat())
        self.assertTrue(sdict['end'] == self.end2.isoformat())
