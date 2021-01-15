import pandas as pd
import numpy as np
import sqlalchemy
from dndata.common.types import *
from sqlalchemy.sql import text, bindparam
from dndata.common.util import db_big_update

# monthly csv info
# EPS(원)
# BPS(원)
# SPS(원)
# CFPS(원)
# EBITDAPS(원)
# PER(배)
# PBR(배)
# PSR(배)
# PCR(배)
# ROA(당기순이익)(%)
# ROE(당기순이익)(%)
# DPS(보통주,현금+주식)(원)
# 배당률(보통주,현금+주식)(%)
# 배당수익률(보통주,현금+주식)(%)
# 당기순이익(천원)
# 배당금(보통주,현금+주식)(천원)
# 8: symbol
# 12: item name

item_info = {
    'eps': {
        'name': 'EPS(원)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'bps': {
        'name': 'BPS(원)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'sps': {
        'name': 'SPS(원)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'cfps': {
        'name': 'CFPS(원)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'ebitdaps': {
        'name': 'EBITDAPS(원)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'per': {
        'name': 'PER(배)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'pbr': {
        'name': 'PBR(배)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'psr': {
        'name': 'PSR(배)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'pcr': {
        'name': 'PCR(배)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'roa': {
        'name': 'ROA(당기순이익)(%)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'roe': {
        'name': 'ROE(당기순이익)(%)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'dps': {
        'name': 'DPS(보통주,현금+주식)(원)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'dividend_ratio': {
        'name': '배당률(보통주,현금+주식)(%)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'dividend_yield': {
        'name': '배당수익률(보통주,현금+주식)(%)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'net_income': {
        'name': '당기순이익(천원)',
        'dtype': np.float64,
        'bind_type': SafeFloat
    },
    'dividend': {
        'name': '배당금(보통주,현금+주식)(천원)',
        'dtype': np.int64,
        'bind_type': SafeBigInt
    },
}

if __name__ == '__main__':
    from dndata.common.dbsession import DBAdaptor, session_scope
    # 123
    uri = 'postgresql://stock-richgo:N41HG7Z0x3EU0PVdY!Os@money-tracer.ct84xmdqlguj.ap-northeast-2.rds.amazonaws.com:5432/stock-richgo'
    adaptor = DBAdaptor(uri)

    data_df = pd.DataFrame()

    for k, v in item_info.items():
        df = pd.read_csv('./financial_monthly_bps.csv', encoding='utf-8-sig', dtype=object)
        df = df.T
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        df = df.set_index(['Symbol', 'Item Name'])
        df = df.drop(df.columns[:10], axis=1)
        df = df.rename(index={v['name']: k})
        df = df.apply(lambda l: l.str.replace('[^-.0-9]', ''))
        df = df.mask(df == '', np.nan).astype(float)
        data_df = pd.concat([data_df, df])

    data_df.columns = data_df.columns.rename('std_dt')

    with session_scope(adaptor, commit=True) as _session:
        total_count = len(data_df.index.levels[0])
        for i, ticker in enumerate(data_df.index.levels[0]):
            temp_df = data_df.loc[ticker].T.dropna(axis=0, how='all').copy()
            temp_df = temp_df.reset_index()
            temp_df['dividend'] *= 1000
            temp_df['net_income'] *= 1000
            temp_df['net_income'] = temp_df['net_income'].mask(temp_df['net_income'] == 0, np.nan)

            temp_df['dpr'] = temp_df['dividend'] / temp_df['net_income'] * 100
            temp_df['ticker'] = ticker

            input_args = temp_df.to_dict(orient='records')

            bind_params = [bindparam('std_dt', type_=sqlalchemy.Date)]
            bind_params += [bindparam(k, type_=v['bind_type']) for k, v in item_info.items()]
            bind_params += [bindparam('dpr', type_=SafeFloat),
                            bindparam('ticker', type_=sqlalchemy.String)]

            columns_str = ','.join(temp_df.columns)
            params_str = ','.join([f':{x}' for x in temp_df.columns])

            stmt = text(f'insert into financial_monthly ({columns_str}) values ({params_str})')
            stmt = stmt.bindparams(*bind_params)
            _session.update(stmt, input_args)
            print(f'{i}/{total_count}')
