from unittest import TestCase
from ..session import *


class TestParse_url_path(TestCase):

    def test_parse_url_path_with_query(self):
        u1 = 'https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2'
        url = parse_url_path(u1)
        self.assertTrue(url == 'paytm.com:443/shop/wallet/txnhistory')

    def test_parse_url_path_basic(self):
        u1 = 'https://paytm.com:443/shop/wallet/txnhistory'
        url = parse_url_path(u1)
        self.assertTrue(url == 'paytm.com:443/shop/wallet/txnhistory')
