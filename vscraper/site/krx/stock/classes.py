from vscraper.site.krx.stock.scraper import KrxFinderScraper, KrxExcelScraper
import pandas as pd


class KrxListedBase(KrxExcelScraper):
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

      종목코드       종목명      현재가      대비 등락률(%)     매도호가     매수호가      거래량(주)         거래대금(원)       시가       고가       저가    액면가    통화구분     상장주식수(주)          상장시가총액(원)
0     060310        3S    2,120      -5   0.24    2,125    2,120      28,120      59,533,215    2,130    2,140    2,105    500  원(KRW)   44,802,511     94,981,323,320
1     095570    AJ네트웍스    4,510       0   0.00    4,515    4,510      17,724      79,980,000    4,570    4,570    4,505  1,000  원(KRW)   46,822,295    211,168,550,450
2     006840     AK홀딩스   26,800    -150   0.56   26,800   26,700      51,253   1,383,875,550   27,000   27,500   26,600  5,000  원(KRW)   13,247,561    355,034,634,800
3     054620    APS홀딩스    7,680     -10   0.13    7,680    7,670      28,203     216,701,570    7,690    7,730    7,630    500  원(KRW)   20,394,221    156,627,617,280
        """
        if date == "":
            date = pd.Timestamp.now().strftime('%Y%m%d')

        result = self.post(indx_ind_cd=market, schdate=date, secugrp=market_detail, stock_gubun=stock_type)
        return pd.read_excel(result, dtype=str)


class KrxListedDetail(KrxFinderScraper):
    @property
    def bld(self):
        return "MKD/13/1302/13020401/mkd13020401"

    def fetch(self, market="ALL", date=""):
        """30009 PER/PBR/배당수익률 (모든종목)
        :param date: 조회 일자 (YYMMDD)
        :param market: 조회 시장 (STK/KSQ/ALL)
        :return:
          bps dvd_yld   end_pr iisu_code  isu_cd     isu_nm                 isu_nm2    pbr     per prv_eps    rn stk_dvd totCnt     work_dt
0       7,142    0.07   68,200         -  000250      삼천당제약   <em class ="up"></em>   9.55  139.75     488     1      50   2378  2020/12/10
1       7,246    1.77    9,050         -  000440     중앙에너비스   <em class ="up"></em>   1.25   68.56     132     2     160         2020/12/10
2         595       0    2,180         -  001000       신라섬유   <em class ="up"></em>   3.66  242.22       9     3       0         2020/12/10
3      11,366    1.71   12,900         -  001540       안국약품   <em class ="up"></em>   1.13   58.37     221     4     220         2020/12/10
4       9,203    0.97    3,590         -  001810       무림SP   <em class ="up"></em>   0.39   58.85      61     5      35         2020/12/10

            BPS (Book-value Per Share): 주당순자산가치 (기업순자산 / 발행주식수)
            dvd_yld (Dividend Yield): 배당수익률
            end_pr (End Price): 종가
            iisu_code (Issue Code): 관리종목 표시
            isu_cd: 종목코드
            isu_nm: 종목명
            pbr (Price-to-Book Ratio): 주가순자산비율 (주가 / BPS)
            per (Price Earnings Ratio): 주가수익률 (주가 / 전년도 주당 순이익)
            prv_eps (Earning Per Share): 전년도 주당 순이익 (당기순이익 / 발행주식수)
        """
        if date == "":
            date = pd.Timestamp.now().strftime('%Y%m%d')

        result = self.post(market_gubun=market, gubun=1, schdate=date)
        return pd.DataFrame(result['result'])


class KrxDeListed(KrxFinderScraper):
    @property
    def bld(self):
        return "MKD/04/0406/04060600/mkd04060600"

    def fetch(self, market="ALL", fromdate="", todate=""):
        """상장 폐지 종목
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param fromdate: 검색 시작일
        :param todate: 검색 종료일
        :return: 종목 검색 결과 DataFrame
    chg_dt   isu_cd  kor_cor_nm                                         tr_stp_rsn
0   2020/11/05  A271780   비엔에프코퍼레이션                                 상장폐지 신청('20.10.13)
1   2020/10/22  A214610     미코바이오메드                                         코스닥시장 이전상장
2   2020/09/23  A126340         비나텍                                         코스닥시장 이전상장
3   2020/09/09  A194510       파티게임즈                                    감사의견거절(감사범위 제한)
        """
        if fromdate == "":
            fromdate = f'{pd.Timestamp.today().year}0101'

        if todate == "":
            todate = f'{pd.Timestamp.today().year}1231'

        result = self.post(market_gubun=market, fromdate=fromdate, todate=todate, del_cd='1')
        return pd.DataFrame(result['block1'])


class KrxChangeListed(KrxFinderScraper):
    @property
    def bld(self):
        return "MKD/04/0406/04060500/mkd04060500_01"

    def fetch(self, market="ALL", fromdate="", todate=""):
        """변경 상장 종목
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param fromdate: 검색 시작일 (입력하지않으면 입력일 기준 년도 1월 1일)
        :param todate: 검색 종료일 (입력하지않으면 입력일 기준 년도 12월 31일)
        :return: 종목 검색 결과 DataFrame
     chg_dt chg_rsn chg_rsn_cd        isu_cd kor_cor_nm   no shrt_isu_cd tot_cnt
0    2020/12/09    상호변경          2  KR7101140002      인바이오젠  181      101140     181
1    2020/12/08    상호변경          2  KR7016250003   SGC이테크건설  180      016250     181
2    2020/12/07    액면변경          3  KR7015230006       대창단조  179      015230     181
3    2020/12/04    액면변경          3  KR7003410008     쌍용양회공업  178      003410     181
        """
        if fromdate == "":
            fromdate = f'{pd.Timestamp.today().year}0101'

        if todate == "":
            todate = f'{pd.Timestamp.today().year}1231'

        result = self.post(market_gubun=market, fromdate=fromdate, todate=todate)
        return pd.DataFrame(result['block1'])


class KrxHoliday(KrxFinderScraper):
    @property
    def bld(self):
        return "MKD/01/0110/01100305/mkd01100305_01"

    def fetch(self, date):
        result = self.post(search_bas_yy=date)
        return pd.DataFrame(result['block1'])


class KrxSupervisedIssues(KrxExcelScraper):
    @property
    def bld(self):
        return "MKD/04/0403/04030100/mkd04030100"

    def fetch(self, market="ALL", fromdate="", todate=""):
        """상장 폐지 종목
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param fromdate: 검색 시작일
        :param todate: 검색 종료일
        :return: 종목 검색 결과 DataFrame
        """
        if fromdate == "":
            fromdate = f'{pd.Timestamp.today().year}0101'

        if todate == "":
            todate = f'{pd.Timestamp.today().year}1231'

        result = self.post(gubun=market, fromdate=fromdate, todate=todate)
        return pd.read_excel(result, dtype=str)


if __name__ == '__main__':
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    import time

    # _df = KrxListedDetail().fetch('STK', date='20201016')
    # _df = KrxAdjPrice().fetch('STK', '20201001', '20201124')
    # _df = KrxListedBase().fetch(stock_type='0', date='20130304')
    # _df2 = KrxListedBase().fetch(market='1001', date='20201123')
    # _df = KrxChangeListed().fetch(fromdate='19991228', todate='20201222')
    # _df = KrxDeListed().fetch(fromdate='19991228', todate='20201222')
    # _df = KrxListedRange().fetch('19991223', '20201124', 'KR7026930008')
    # _df = KrxListedRange().fetch('19991223', '20201124', 'KR7005930003')
    # _df = KrxSupervisedIssues().fetch('STK', '20100101', '20110101')
    _df = KrxListedInfo().fetch('A005930', '19991228', '20201224')
    print(_df)
    # 1001 - 코스피
    # 2001 - 코스닥
    # _df = KrxListedBase().fetch(stock_type='0', date='19991228')
    # _df['date'] = '19991228'
    # _df.to_csv(f'200001.csv', mode='a', encoding='utf-8-sig', header=False)

    # for x in pd.date_range(pd.Timestamp('20000101'), pd.Timestamp('20201222'), freq='M'):
    # for x in pd.date_range(pd.Timestamp('20201231'), pd.Timestamp('20210101'), freq='M'):
    #     month_start = x.strftime('%Y%m01')
    #     month_end = x
    #     filename = x.strftime('%Y%m')
    #
    #     for y in pd.date_range(month_start, month_end):
    #         if y.dayofweek in [5, 6]:
    #             continue
    #
    #         print(y)
    #
    #         _df = KrxListedBase().fetch(stock_type='0', date=y.strftime('%Y%m%d'))
    #         if _df.empty:
    #             continue
    #
    #         _df['date'] = y.strftime('%Y%m%d')
    #         _df.to_csv(f'{filename}.csv', mode='a', encoding='utf-8-sig', header=False)
    #         print('success')


