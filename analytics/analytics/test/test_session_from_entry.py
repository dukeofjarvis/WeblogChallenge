from unittest import TestCase
from ..session import *

class TestSession_from_entry(TestCase):

    start1 = datetime(2016, 02, 28, 2, 0, 1)
    start2 = datetime(2016, 02, 28, 2, 30, 1)

    end1 = datetime(2016, 02, 28, 2, 0, 1)
    end2 = datetime(2016, 02, 28, 3, 0, 1)

    request1_str = 'GET http://test.com/url/'
    request2_str = 'POST http://test.com/other/'

    request1 = {'method': 'GET', 'url': 'http://test.com/url/'}
    request2 = {'method': 'POST', 'url': 'http://test.com/other/'}

    entry = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"

    def test_session_from_entry(self):
        s = session_from_entry(self.entry)
        self.assertTrue(s['ip'] == '123.242.248.130')
        self.assertTrue(s['start'] == iso8601.parse_date('2015-07-22T09:00:28.019143Z'))
        self.assertTrue(s['end'] == iso8601.parse_date('2015-07-22T09:00:28.019143Z'))
        self.assertTrue(s['requests'][0]['method'] == 'GET')
        self.assertTrue(s['requests'][0]['url'] == 'https://paytm.com:443/shop')