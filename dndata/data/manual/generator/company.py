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

    df = df.rename(columns={'Symbol': 'ticker',
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


def get_refined_krx_daily(key):
    df = pd.read_csv(f'{RESOURCE_DIR}/krx_daily/{key}.csv', header=None, index_col=0)

    df.columns = ['종목코드', '종목명', '현재가', '대비', '등락률(%)', '매도호가', '매수호가', '거래량(주)', '거래대금(원)', '시가',
                  '고가', '저가', '액면가', '통화구분', '상장주식수(주)', '상장시가총액(원)', '날짜']

    df = df.rename(columns={
        '종목코드': 'ticker', '종목명': 'name', '현재가': 'closing_pr', '대비': 'change_pr', '등락률(%)': 'return',
        '매도호가': 'ask', '매수호가': 'bid', '거래량(주)': 'volume', '거래대금(원)': 'trading_val', '시가': 'open_pr',
        '고가': 'high_pr', '저가': 'low_pr', '액면가': 'face_val', '통화구분': 'currency', '상장주식수(주)': 'issued_sh',
        '상장시가총액(원)': 'market_cap', '날짜': 'std_dt'
    })

    df['ticker'] = df['ticker'].map(lambda l: f'A{l:06d}')

    return df


# krx 파일을 column 별로 나눠서 csv 로 저장. (이미 저장되어있다면 아래쪽에 붙임)
def merge_krx_daily(key, code_list, header=False):
    df = get_refined_krx_daily(key)

    int_items = ['closing_pr', 'change_pr', 'ask', 'bid', 'volume', 'trading_val', 'open_pr', 'high_pr', 'low_pr',
                 'issued_sh', 'market_cap']

    float_items = ['face_val']

    for x in int_items:
        df[x] = df[x].fillna('0').str.replace(',', '').astype(np.int64)

    for x in float_items:
        df[x] = df[x].fillna('0.0').str.replace(',', '').astype(np.float)

    df['return'] = 0

    float_items = ['return']
    for x in float_items:
        df[x] = df[x].astype(float)

    df['currency'] = df['currency'].str.replace('[^A-Za-z]', '')

    for x in df.columns[1:-1]:
        temp_df = df.pivot(index='std_dt', columns='ticker', values=x)
        temp_df = temp_df.reindex(columns=code_list)
        temp_df.to_csv(f'{RESOURCE_DIR}/krx_output/krx_{x}.csv', encoding='utf-8-sig', mode='a', header=header)


# krx csv 데이터에서 전체 code list 를 파일로 저장
def krx_make_code_list_file():
    code_list = []
    for x in pd.date_range('20000101', '20210101', freq='M'):
        year_month = x.strftime('%Y%m')
        _df = get_refined_krx_daily(year_month)
        code_list = list(set(code_list + _df['ticker'].values.tolist()))

    code_list = list(sorted(code_list))
    with open(f'{RESOURCE_DIR}/krx_output/code_list.txt', 'w') as fp:
        fp.write(','.join(code_list))


# 전체 코드리스트 파일에서 읽어옴
def krx_load_all_code_list():
    with open(f'{RESOURCE_DIR}/krx_output/code_list.txt', 'r') as fp:
        code_list = fp.read()
        code_list = code_list.split(',')
    return code_list


def filter_change(base_file_path, item_name, dtype):
    out_file_name = f'changed_{item_name}.csv'
    csv_df = pd.read_csv(base_file_path, index_col=0, dtype=object)
    if dtype == str:
        csv_df = csv_df.fillna('')
        csv_df = csv_df.astype(str)
    elif dtype == int:
        csv_df = csv_df.astype(np.float64).fillna(0).astype(np.int64).astype(str)
    elif dtype == float:
        csv_df = csv_df.astype(np.float64).fillna(0).astype(str)
    else:
        raise

    df = pd.DataFrame(columns=['std_dt', 'ticker', 'value', 'item'])

    def _append_item(_sr, _name, _container_df):
        _sr = _sr[_sr.values != _sr.shift(1).values].dropna()
        _df = _sr.to_frame().stack().reset_index()
        _df['item'] = _name
        _df.columns = ['std_dt', 'ticker', 'value', 'item']
        _container_df = _container_df.append(_df, ignore_index=True)
        return _container_df

    for x in csv_df.columns:
        df = _append_item(csv_df[x], item_name, df)

    if isinstance(df['std_dt'][0], int):
        df['std_dt'] = df['std_dt'].astype(str)
    if isinstance(df['std_dt'][0], float):
        df['std_dt'] = df['std_dt'].astype(int).astype(str)
    else:
        df['std_dt'] = pd.to_datetime(df['std_dt']).dt.strftime('%Y%m%d')

    df = df.pivot(index=['std_dt', 'ticker'], columns='item', values='value').sort_index(level=1).ffill()
    df.reset_index().to_csv(f'{RESOURCE_DIR}/result/{out_file_name}', encoding='utf-8-sig')


def filter_change_delisted(base_file_path):
    out_file_name = f'changed_delisted.csv'
    csv_df = pd.read_csv(base_file_path, index_col=0, dtype=object)
    csv_df = csv_df.rename(columns={'isu_cd': 'ticker', 'chg_dt': 'std_dt', 'tr_stp_rsn': 'value'})
    csv_df = csv_df[['std_dt', 'ticker', 'value']]

    csv_df = csv_df[~csv_df.apply(lambda l: '이전상장' in l['value'], axis=1)]
    csv_df = csv_df[~csv_df.apply(lambda l: '유가증권' in l['value'], axis=1)]
    csv_df = csv_df[~csv_df.apply(lambda l: '증권거래소' in l['value'], axis=1)]

    csv_df['std_dt'] = pd.to_datetime(csv_df['std_dt'])
    csv_df['std_dt'] = csv_df['std_dt'].dt.strftime('%Y%m%d')
    csv_df = csv_df.set_index(['std_dt', 'ticker'])

    csv_df.reset_index().to_csv(f'{RESOURCE_DIR}/result/{out_file_name}', encoding='utf-8-sig')


# 값이 달라진 항목을 걸러낸다 (일자, 항목, to)
def filter_krx_item():
    face_df = pd.read_csv(f'{RESOURCE_DIR}/krx_output/krx_face_val.csv', index_col=0, dtype=object)
    face_df.index = face_df.index.astype(str)
    name_df = pd.read_csv(f'{RESOURCE_DIR}/krx_output/krx_name.csv', index_col=0, dtype=object)
    name_df.index = name_df.index.astype(str)
    currency_df = pd.read_csv(f'{RESOURCE_DIR}/krx_output/krx_currency.csv', index_col=0, dtype=object)
    currency_df.index = currency_df.index.astype(str)
    issued_sh_df = pd.read_csv(f'{RESOURCE_DIR}/krx_output/krx_issued_sh.csv', index_col=0, dtype=object)
    issued_sh_df.index = issued_sh_df.index.astype(str)

    df = pd.DataFrame(columns=['std_dt', 'ticker', 'value', 'item'])

    def _append_item(_sr, _name, _container_df):
        _sr = _sr[_sr.values != _sr.shift(1).values].dropna()
        _df = _sr.to_frame().stack().reset_index()
        _df['item'] = _name
        _df.columns = ['std_dt', 'ticker', 'value', 'item']
        _container_df = _container_df.append(_df, ignore_index=True)
        return _container_df

    for x in face_df.columns:
        df = _append_item(face_df[x], 'face_val', df)
        df = _append_item(name_df[x], 'name', df)
        df = _append_item(currency_df[x], 'currency', df)
        df = _append_item(issued_sh_df[x], 'issued_sh', df)

    df = df.pivot(index=['std_dt', 'ticker'], columns='item', values='value').sort_index(level=1).ffill()
    df.reset_index().to_csv('filter_result.csv', encoding='utf-8-sig')


def filter_data_guide_item():
    face_df = pd.read_csv(f'{RESOURCE_DIR}/dg_output/face_value.csv', dtype=object)
    issued_sh_df = pd.read_csv(f'{RESOURCE_DIR}/dg_output/issued_shares.csv', dtype=object)

    face_df = face_df.set_index('Unnamed: 0').astype(int).sort_index(axis=1)
    face_df.index = pd.to_datetime(face_df.index).strftime('%Y%m%d')

    issued_sh_df = issued_sh_df.set_index('Unnamed: 0').astype(int).sort_index(axis=1)
    issued_sh_df.index = pd.to_datetime(issued_sh_df.index).strftime('%Y%m%d')

    df = pd.DataFrame(columns=['std_dt', 'ticker', 'value', 'item'])

    def _append_item(_sr, _name, _container_df):
        _sr = _sr[_sr.values != _sr.shift(1).values].dropna()
        _df = _sr.to_frame().stack().reset_index()
        _df['item'] = _name
        _df.columns = ['std_dt', 'ticker', 'value', 'item']
        _container_df = _container_df.append(_df, ignore_index=True)
        return _container_df

    for i, x in enumerate(face_df.columns):
        print(i)
        df = _append_item(face_df[x], 'face_val', df)
        df = _append_item(issued_sh_df[x], 'issued_sh', df)

    df = df.pivot(index=['std_dt', 'ticker'], columns='item', values='value').sort_index(level=1).ffill()
    df = df.reset_index()

    # 상장되지않은 상태인 데이터 제거
    drop_indexes = df.reset_index().query('face_val == 0 and issued_sh == 0 and std_dt == "19991228"').index
    df = df.drop(drop_indexes)

    df.reset_index().to_csv(f'{RESOURCE_DIR}/dg_output/filter_result.csv', encoding='utf-8-sig', index=False)


def cleansing_data_guide_company():
    face_df = pd.read_csv(f'{RESOURCE_DIR}/dg_basic/face_value.csv', index_col=0, dtype=object)
    face_df = face_df.fillna(0).astype(str).apply(lambda l: l.str.replace(',', ''))
    face_df.to_csv(f'{RESOURCE_DIR}/dg_output/face_value.csv', encoding='utf-8-sig')

    issued_sh_df = pd.read_csv(f'{RESOURCE_DIR}/dg_basic/issued_shares.csv', index_col=0, dtype=object)
    issued_sh_df = issued_sh_df.fillna(0).astype(str).apply(lambda l: l.str.replace(',', ''))
    issued_sh_df.to_csv(f'{RESOURCE_DIR}/dg_output/issued_shares.csv', encoding='utf-8-sig')


def generate_complete_company_data():
    # ticker
    # isin
    # name
    # init_listed_dt
    # listed_dt
    # changed_dt
    # delisted_dt
    # changed_rsn (delisted 포함)
    # issued_sh
    # face_val
    # market
    # currency
    # min_unit
    # trd_unit
    # created_at
    # updated_at
    pass


if __name__ == '__main__':
    pass
    # cleansing_company_raw_data()
    # merge_change_listed_and_delisted()

    # 코드리스트를 만든다
    # krx_make_code_list_file()

    # 파일을 나눈다
    # _code_list = krx_load_all_code_list()
    # merge_krx_daily('200001', _code_list, header=True)
    #
    # for _x in pd.date_range('20000201', '20210101', freq='M'):
    #     _year_month = _x.strftime('%Y%m')
    #     print(_year_month)
    #     merge_krx_daily(_year_month, _code_list)

    # * Symbol: ticker
    # * Ticker: ticker
    # * ISIN: isin
    # * 최초상장일: init_listed_dt (initial listing date)
    # 종목명: name
    # 상장일: listed_dt
    # 변경일: changed_dt
    # 폐지일: delisted_dt
    # 변경사유 (종목명변경, 시장이전, 발생주식증감, 액면가변경 등.): changed_reason
    #        종목명변경: krx, 시장이전, 발생주식증감, 액면가변경: data guide
    # 발행주식수: issued_sh (issued shares)
    # 액면가: face_val (face value)
    # 시장 (KOSPI, KOSDAQ 등): market
    # 통화 (KRW): currency
    # * 최소매매단위: min_unit (minimum trading units)
    # * 매매단위: trd_unit (trading units)

    # 값이 달라진 항목을 걸러낸다 (dataguide company list)
    # 종목명 (krx_name.csv)
    # 발행주식수 (issued_sh.csv)
    # 액면가 (krx_face_val.csv)
    # 통화 (krx_currency.csv)

    # 시장 (dataguide/market.csv)
    # KOSPI 200 편입여부
    # KOSPI 100 편입여부
    # - 최초상장일 (initial_listed_date.csv)
    # 상장폐지일 (dataguide/market.csv)
    # 상장일 (market)

    # 국가
    #

    # KRX FILTER
    # filter_change(f'{RESOURCE_DIR}/krx_output/krx_name.csv', 'name', str)
    # filter_change(f'{RESOURCE_DIR}/krx_output/krx_issued_sh.csv', 'issued_sh', int)
    # filter_change(f'{RESOURCE_DIR}/krx_output/krx_face_val.csv', 'face_val', float)
    # filter_change(f'{RESOURCE_DIR}/krx_output/krx_currency.csv', 'currency', str)

    # DATA GUIDE FILTER
    # filter_change(f'{RESOURCE_DIR}/dataguide/market.csv', 'market', str)
    # filter_change(f'{RESOURCE_DIR}/dataguide/kospi200_historical.csv', 'kospi200', str)
    # filter_change(f'{RESOURCE_DIR}/dataguide/kospi100_historical.csv', 'kospi100', str)
    # filter_change_delisted(f'{RESOURCE_DIR}/krx_basic/delisted.csv')
    # filter_change(f'{RESOURCE_DIR}/dataguide/administrative_issues.csv', 'administrative_issues', str)
    # filter_change(f'{RESOURCE_DIR}/dataguide/administrative_issues_reason.csv', 'administrative_issues_reason', str)
    filter_change(f'{RESOURCE_DIR}/dataguide/issues_suspended_from_trading.csv', 'issues_suspended_from_trading', str)
    filter_change(f'{RESOURCE_DIR}/dataguide/issues_suspended_from_trading_reason.csv', 'issues_suspended_from_trading_reason', str)

    # 값이 달라진 항목을 걸러낸다 (액면가, 상장주식수)
    # cleansing_data_guide_company()
    # filter_data_guide_item()

    # 가능한 종목 기초데이터 적재
    # 종목별 루프 돌면서 기존 적재된 데이터와 다를 시 changed_reason 에 기록
    #
    #     csv_df = pd.read_csv(f'{RESOURCE_DIR}/result/changed_issued_sh.csv', index_col=0, dtype=object)
    #     csv_df['std_dt'] = csv_df['std_dt'].astype(float).astype(int).astype(str)
    #
    #     csv_df.to_csv(f'{RESOURCE_DIR}/result/changed_issued_sh.csv', encoding='utf-8-sig')

    kkk = 0
