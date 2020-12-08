from .scraper import NaverScraper

__all__ = [
    'NaverOHLCV'
]


class NaverOHLCV(NaverScraper):
    @property
    def uri(self):
        return "/sise.nhn"

    def fetch(self, ticker, count, freq='day') -> str:
        result = self.read(symbol=ticker, timeframe=freq, count=count, requestType="0")
        return result.text
