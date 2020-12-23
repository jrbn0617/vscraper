import pandas as pd
import numpy as np
from dndata import RESOURCE_DIR


def cleansing_company_raw_data():
    df = pd.read_excel(f'{RESOURCE_DIR}/company.xlsx', header=5, sheet_name=0)

    # 스펙 제거 (DataGuide)
    df = df[df['종목명 (Full)'].map(lambda l: not pd.isnull(l) and '인수목적' not in l)]

    df = df[[
        'Symbol',
        'Name',
        '국제표준코드',
        'Ticker',
        '종목명 (영문)',
        '상장(등록)일자',
        '통화(ISO)',
        '국가'
    ]]

    df = df.rename(columns={'Symbol': 'symbol',
                            'Name': 'name',
                            '국제표준코드': 'isin',
                            'Ticker': 'ticker',
                            '종목명 (영문)': 'name_en',
                            '상장(등록)일자': 'init_listed_dt',
                            '통화(ISO)': 'currency',
                            '국가': 'country'})

    df.to_csv(f'{RESOURCE_DIR}/adjusted_company.csv', encoding='utf-8-sig')


def merge_change_listed_and_delisted():
    temp_df = pd.read_csv(f'{RESOURCE_DIR}/adjusted_company.csv')
    isin_sr = temp_df.set_index('ticker')['isin']
    ticker_sr = temp_df.set_index('isin')['ticker']

    df_1 = pd.read_csv(f'{RESOURCE_DIR}/change_listed.csv')
    df_2 = pd.read_csv(f'{RESOURCE_DIR}/delisted.csv')
    old_df2 = df_2.copy()

    # --- change listed 정리 (KRX)
    df_1 = df_1[['isu_cd', 'chg_dt', 'chg_rsn']]
    df_1 = df_1.rename(columns={'isu_cd': 'isin', 'chg_dt': 'changed_dt', 'chg_rsn': 'changed_reason'})
    df_1['delisted_dt'] = np.nan
    df_1['changed_dt'] = pd.to_datetime(df_1['changed_dt'])
    # kospi, kosdaq 에 속하지 않는 데이터 제거
    df_1 = df_1.loc[df_1['isin'].map(ticker_sr).dropna().index]
    # 상호변경 만 남기고 제거
    check_reason = ['상호변경']
    df_1 = df_1.query('changed_reason in @check_reason')

    # --- delisted 정리 (KRX)
    df_2 = df_2[['isu_cd', 'chg_dt', 'tr_stp_rsn']]
    df_2['isu_cd'] = df_2['isu_cd'].map(isin_sr)
    df_2 = df_2.rename(columns={'isu_cd': 'isin', 'chg_dt': 'changed_dt', 'tr_stp_rsn': 'changed_reason'})
    df_2['changed_dt'] = pd.to_datetime(df_2['changed_dt'])
    df_2['delisted_dt'] = df_2['changed_dt']
    # kospi, kosdaq 에 속하지 않는 데이터 제거
    df_2 = df_2.loc[df_2['isin'].dropna().index]
    # 이전 상장 정보 제거
    ignore_reason = ['코스닥시장 이전상장', '코스닥시장 상장', '유가증권시장 상장', '증권거래소 상장', '한국증권거래소 상장']
    df_2 = df_2.query('changed_reason not in @ignore_reason')

    df_1 = df_1.set_index(['isin', 'changed_dt'])
    df_2 = df_2.set_index(['isin', 'changed_dt'])

    kkk = 0


def merge_krx_basic(key):
    # columns:
    df = pd.read_csv(f'{RESOURCE_DIR}/krx_basic/{key}.csv', header=None, index_col=0)

    df.columns = ['종목코드', '종목명', '현재가', '대비', '등락률(%)', '매도호가', '매수호가', '거래량(주)', '거래대금(원)', '시가',
                  '고가', '저가', '액면가', '통화구분', '상장주식수(주)', '상장시가총액(원)', '날짜']

    df = df.rename(columns={
        '종목코드': 'symbol', '종목명': 'name', '현재가': 'closing_pr', '대비': 'change_pr', '등락률(%)': 'return',
        '매도호가': 'ask', '매수호가': 'bid', '거래량(주)': 'volume', '거래대금(원)': 'trading_val', '시가': 'open_pr',
        '고가': 'high_pr', '저가': 'low_pr', '액면가': 'face_val', '통화구분': 'currency', '상장주식수(주)': 'issued_sh' ,
        '상장시가총액(원)': 'market_cap', '날짜': 'trading_dt'
    })

    df['symbol'] = df['symbol'].map(lambda l: f'A{l:06d}')

    # df['trading_dt'] = pd.to_datetime(df['trading_dt'].astype(str))
    int_items = ['closing_pr', 'change_pr', 'ask', 'bid', 'volume', 'trading_val', 'open_pr', 'high_pr', 'low_pr',
                 'face_val', 'issued_sh', 'market_cap']

    for x in int_items:
        df[x] = df[x].fillna('0').str.replace(',', '').astype(np.int64)

    float_items = ['return']
    for x in float_items:
        df[x] = df[x].astype(float)

    df['currency'] = df['currency'].str.replace('[^A-Za-z]', '')

    for x in df.columns[1:-1]:
        temp_df = df.pivot(index='trading_dt', columns='symbol', values=x)
        temp_df.to_csv(f'{RESOURCE_DIR}/krx_output/krx_{x}.csv', encoding='utf-8-sig', mode='a')

# * Symbol: symbol
# * Ticker: ticker
# * ISIN: isin
# * 종목명: name
# * 종목명(영문): name_en
# * 최초상장일: init_listed_dt (initial listing date)
# 상장일: listed_dt
# 변경일: changed_dt
# 폐지일: delisted_dt
# 변경사유 (종목명변경, 시장이전, 발생주식증감, 액면가변경 등.): changed_reason
# 발행주식수: issued_sh (issued shares)
# 액면가: face_val (face value)
# * 최소매매단위: min_unit (minimum trading units)
# * 매매단위: trd_unit (trading units)
# 시장 (KOSPI, KOSDAQ 등): market
# 국가 (KOREA): country
# 통화 (KRW): currency

# symbol name isin market

if __name__ == '__main__':
    # cleansing_company_raw_data()
    # merge_change_listed_and_delisted()
    merge_krx_basic(200001)
