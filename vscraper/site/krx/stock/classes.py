from vscraper.site.krx.stock.scraper import KrxFinderScraper, KrxExcelScraper
import pandas as pd


class ListedScraper(KrxExcelScraper):
    @property
    def bld(self):
        return "MKD/04/0406/04060200/mkd04060200"

    def fetch(self, market="", market_detail="ST", stock_type="ON", date=""):
        """
        일자별 시세 스크래핑에서 종목 검색기
        :param market: 조회 시장
                 - 전체
            1001 - 코스피
            2001 - 코스닥
            N001 - 코넥스
        :param market_detail:
            ST - 주식시장
            EF - ETF시장
            EW - ELW시장
            EN - ETN시장

        :param stock_type: 주식종류
            ON - 전체
            0 - 보통주
            9 - 종류주식
        :param date: 검색기준일 (입력하지않으면 요청 당일)
        :return: 종목 검색 결과 DataFrame
        """
        if date == "":
            date = pd.Timestamp.now().strftime('%Y%m%d')

        result = self.post(indx_ind_cd=market, schdate=date, secugrp=market_detail, stock_gubun=stock_type)
        return pd.read_excel(result, dtype=str)


class DeListedScraper(KrxFinderScraper):
    @property
    def bld(self):
        return "MKD/04/0406/04060600/mkd04060600"

    def fetch(self, market="ALL", fromdate="", todate=""):
        """상장 폐지 종목
        http://marketdata.krx.co.kr/mdi#document=040603
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param fromdate: 검색 시작일
        :param todate: 검색 종료일
        :return: 종목 검색 결과 DataFrame
        """
        if fromdate == "":
            fromdate = f'{pd.Timestamp.today().year}0101'

        if todate == "":
            todate = f'{pd.Timestamp.today().year}1231'

        result = self.post(market_gubun=market, fromdate=fromdate, todate=todate, del_cd='1')
        return pd.DataFrame(result['block1'])


class ChangeListedScraper(KrxFinderScraper):
    @property
    def bld(self):
        return "MKD/04/0406/04060500/mkd04060500_01"

    def fetch(self, market="ALL", fromdate="", todate=""):
        """상장 폐지 종목
        http://marketdata.krx.co.kr/mdi#document=040603
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param fromdate: 검색 시작일
        :param todate: 검색 종료일
        :return: 종목 검색 결과 DataFrame
        """
        if fromdate == "":
            fromdate = f'{pd.Timestamp.today().year}0101'

        if todate == "":
            todate = f'{pd.Timestamp.today().year}1231'

        result = self.post(market_gubun=market, fromdate=fromdate, todate=todate)
        return pd.DataFrame(result['block1'])


if __name__ == '__main__':
    # _df = StockFinder().fetch()
    # _df.to_csv('listing.csv', encoding='utf-8-sig')
    _df = ListedScraper().fetch(date='20201102')
    print(_df)
