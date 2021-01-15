import pandas as pd
import numpy as np
import sqlalchemy
from dndata.common.types import *
from sqlalchemy.sql import text, bindparam
from dndata.common.util import db_big_update

item_info = {
    'adj_price': {
        'name': '수정주가 (현금배당반영)(원)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'amount': {
        'name': '거래대금(원)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'dividend_ratio': {
        'name': '배당수익률(GAAP-개별)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'issued_shares': {
        'name': '상장주식수(주)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'market_cap': {
        'name': '시가총액 (상장예정주식수 포함)(백만원)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'npp_f': {
        'name': '순매수대금(외국인계)(만원)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'npp_i': {
        'name': '순매수대금(기관계)(만원)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'npp_r': {
        'name': '순매수대금(개인)(만원)',
        'dtype': int,
        'bind_type': SafeInteger
    },
    'pbr': {
        'name': 'PBR(GAAP-개별)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'pcr': {
        'name': 'PCR(GAAP-개별)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'per': {
        'name': 'PER(GAAP-개별)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'psr': {
        'name': 'PSR(GAAP-개별)',
        'dtype': float,
        'bind_type': SafeFloat
    },
    'volume': {
        'name': '거래량(주)',
        'dtype': int,
        'bind_type': SafeInteger
    }
}

if __name__ == '__main__':
    from dndata.common.dbsession import DBAdaptor, session_scope
    import time

    uri = 'postgresql://stock-richgo:N41HG7Z0x3EU0PVdY!Os@money-tracer.ct84xmdqlguj.ap-northeast-2.rds.amazonaws.com:5432/stock-richgo'
    adaptor = DBAdaptor(uri)

    # data_df = pd.DataFrame()
    #
    # s = time.time()
    #
    # for i, (k, v) in enumerate(item_info.items()):
    #     print(f'{i}/{len(item_info)}')
    #     df = pd.read_csv(f'./stock_{k}.csv', encoding='utf-8-sig', dtype=object)
    #     df = df.T
    #     df.columns = df.iloc[0]
    #     df = df.iloc[1:]
    #     df = df.set_index(['Symbol', 'Item Name'])
    #     df = df.drop(df.columns[:10], axis=1)
    #     df = df.rename(index={v['name']: k})
    #     df = df.apply(lambda l: l.str.replace('[^-.0-9]', ''))
    #     df = df.mask(df == '', np.nan).astype(float)
    #     data_df = pd.concat([data_df, df])
    #
    # data_df.columns = data_df.columns.rename('std_dt')
    #
    # data_df.to_pickle('temp.pickle')
    #
    # print(time.time() - s)
    data_df = pd.read_pickle('./stock_daily.pickle')
    kkk = 0
    #
    # with session_scope(adaptor, commit=True) as _session:
    #     total_count = len(data_df.index.levels[0])
    #     for i, ticker in enumerate(data_df.index.levels[0]):
    #         temp_df = data_df.loc[ticker].T.dropna(axis=0, how='all').copy()
    #         temp_df = temp_df.reset_index()
    #         temp_df['dividend'] *= 1000
    #         temp_df['net_income'] *= 1000
    #         temp_df['net_income'] = temp_df['net_income'].mask(temp_df['net_income'] == 0, np.nan)
    #
    #         temp_df['dpr'] = temp_df['dividend'] / temp_df['net_income'] * 100
    #         temp_df['ticker'] = ticker
    #
    #         input_args = temp_df.to_dict(orient='records')
    #
    #         bind_params = [bindparam('std_dt', type_=sqlalchemy.Date)]
    #         bind_params += [bindparam(k, type_=v['bind_type']) for k, v in item_info.items()]
    #         bind_params += [bindparam('dpr', type_=SafeFloat),
    #                         bindparam('ticker', type_=sqlalchemy.String)]
    #
    #         columns_str = ','.join(temp_df.columns)
    #         params_str = ','.join([f':{x}' for x in temp_df.columns])
    #
    #         stmt = text(f'insert into financial_monthly ({columns_str}) values ({params_str})')
    #         stmt = stmt.bindparams(*bind_params)
    #         _session.update(stmt, input_args)
    #         print(f'{i}/{total_count}')
