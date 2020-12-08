from vscraper.common.requests import RequestGet


class NaverScraper(RequestGet):
    @property
    def url(self):
        return "http://fchart.stock.naver.com/sise.nhn"
