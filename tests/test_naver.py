import unittest
from vscraper.site.naver.classes import *
from vscraper.site.naver.api import *


class TestNaver(unittest.TestCase):
    def test_classes(self):
        result = NaverOHLCV().fetch("006800", 10, "week")
        self.assertNotEqual(result, '')

    def test_api(self):
        df = NaverAPI.get_ohlcv("20201101", "20201201", "000020")
        self.assertNotEqual(df.empty, True)
