from vscraper.common.util import singleton
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np

from .classes import NaverOHLCV
from vscraper.api.interface import ScraperAPI


__all__ = [
    'NaverAPI'
]


class NaverAPI(ScraperAPI):
    @staticmethod
    def get_ohlcv(from_date, to_date, ticker):
        from_date = pd.Timestamp(from_date)
        to_date = pd.Timestamp(to_date)
        today = pd.Timestamp.now()
        elapsed = today - from_date
        xml = NaverOHLCV().fetch(ticker, elapsed.days)

        result = []
        for node in et.fromstring(xml).iter(tag='item'):
            row = node.get('data')
            result.append(row.split("|"))

        cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(result, columns=cols).set_index('date')
        df.index = pd.to_datetime(df.index)

        return df[from_date:to_date].astype(int)
