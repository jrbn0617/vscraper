import requests
from abc import abstractmethod
from lxml.html import fromstring
from itertools import cycle


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    _proxies = set()
    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            # Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            _proxies.add(proxy)
    return _proxies


proxies = get_proxies()
proxy_pool = cycle(proxies)


class RequestGet:
    def read(self, proxy=None, **params):
        resp = requests.get(self.url, headers=self.headers, params=params,
                            proxies={'http': proxy, 'https': proxy})
        return resp

    @property
    def headers(self):
        return {"User-Agent": "Mozilla/5.0"}

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


class RequestPost:
    def read(self, proxy=None, **params):
        resp = requests.post(self.url, headers=self.headers, data=params,
                             proxies={'http': proxy, 'https': proxy})
        return resp

    @property
    def headers(self):
        return {"User-Agent": "Mozilla/5.0"}

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError
