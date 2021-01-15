import sqlalchemy
from dndata.common.dbsession import DBAdaptor, session_scope
import pandas as pd
import numpy as np
import os
from collections import OrderedDict

db_uri = 'mysql+pymysql://root:1234@localhost:3306/dnstock'
db_adaptor = DBAdaptor(db_uri)


def parse_sql_script(script_path):
    with open(script_path, 'rb') as fp:
        sql_text = fp.read().decode("UTF-8")

    sql_text = sql_text.split('\n')
    container = []
    sql_command = ''

    # Iterate over all lines in the sql file
    for line in sql_text:
        # Ignore commented lines
        if not (line.startswith('--') or line.startswith('#')) and line.strip('\n'):
            # Append line to the command string
            sql_command += line.strip('\n')

            # If the command string ends with ';', it is a full statement
            if sql_command.endswith(';'):
                container.append(sql_command)
                sql_command = ''

    return container


def setup_tables():
    sql_command_list = parse_sql_script('./resource/DDL_mysql.sql')

    with session_scope(db_adaptor, commit=True) as session:
        for x in sql_command_list:
            stmt = sqlalchemy.text(x)
            session.update(stmt)


def apply_changed_listed(operation_df):
    operation_df = operation_df.copy()
    drop_market = ['K-OTC', 'KONEX']
    valid_market = ['KOSDAQ', 'KOSPI']
    active_market = operation_df.iloc[0]['market']
    active_market_start_date = operation_df.index[0]
    active_market_end_date = ''

    drop_item_list = []
    for s, e in zip(operation_df.index[:-1], operation_df.index[1:]):
        a_market = operation_df.loc[s, 'market']
        b_market = operation_df.loc[e, 'market']

        if active_market != b_market:
            if active_market_start_date != operation_df.index[0]:
                active_market_start_date = active_market_end_date

            active_market_end_date = s
            active_market = b_market

        if a_market in drop_market and b_market in valid_market:
            # drop -> valid
            drop_item_list += operation_df.loc[active_market_start_date:active_market_end_date].index.tolist()
            operation_df.loc[e, 'changed_reason'] = '[이전상장]'
            operation_df.loc[e, 'listed_dt'] = e
            operation_df.loc[e, 'delisted_dt'] = np.nan

        elif b_market in drop_market and a_market in valid_market:
            # valid -> drop
            raise

    operation_df = operation_df.drop(drop_item_list)

    return operation_df


def insert_kr_stock_operation():
    # container 생성
    column_order = [
        'dn_id',
        'ticker',
        'isin',
        'init_listed_dt',
        'listed_dt',
        'changed_dt',
        'end_dt',
        'delisted_dt',
        'changed_reason',
        'company_name',
        # 'issued_shares',
        'face_value',
        'market',
        'currency',
        'min_order',
        'trading_unit',
    ]

    def _ticker_to_dnid(_ticker):
        # KR + Stock
        return 'KRS-' + _ticker

    def _get_data(_data_id, _ticker):
        _data = getattr(_get_data, _data_id, None)
        if _data is None:
            _data = pd.read_csv(f'./resource/{_data_id}.csv', index_col=0, dtype=str)
            _is_historical = all(np.isin(['ticker', 'std_dt'], _data.columns))
            if _is_historical:
                _data = _data.set_index(['ticker', 'std_dt'])

            setattr(_get_data, _data_id, _data)

        _has_ticker = False
        if _data.index.nlevels > 1:
            if _ticker not in _data.index.levels[0]:
                return pd.DataFrame(columns=_data.columns)
        else:
            if _ticker not in _data.index:
                return pd.DataFrame(columns=_data.columns)

        return _data.loc[_ticker]

    listed_df = pd.read_csv('./resource/initial_listed_date.csv', index_col=0)
    listed_df = listed_df.dropna().astype(int)

    # 초기화. isin 을 기반으로 ticker list 세팅
    ticker_list = listed_df.index.tolist()

    def _generate_history(_ticker):
        _dn_id = _ticker_to_dnid(_ticker)

        _init_listed_dt = _get_data('initial_listed_date', _ticker).to_frame().T
        _init_listed_dt['std_dt'] = _init_listed_dt['init_listed_dt']
        _init_listed_dt = _init_listed_dt.set_index('std_dt')
        _init_listed_dt['changed_reason'] = np.nan

        _isin = _get_data('isin', _ticker).to_frame().T
        _isin['std_dt'] = _init_listed_dt['init_listed_dt'].values
        _isin = _isin.set_index('std_dt')

        def _diff_prev(_item, _cont, _reason):
            if len(_cont) > 1:
                iter_list = list(range(len(_cont)))
                for _x, _y in zip(iter_list[0:-1], iter_list[1:]):
                    a = _cont[_item].iloc[_x]
                    b = _cont[_item].iloc[_y]
                    if a != b:
                        _cont.loc[_cont.index[_y], 'changed_reason'] = _reason

        def _diff_simple(_item, _cont, _reason):
            for _std_dt, _sr in _cont.iterrows():
                if _std_dt != '19991228' and (_sr[_item] != '0' or not pd.isnull(_sr[_item])):
                    _cont.loc[_std_dt, 'changed_reason'] = _reason

        # 상장일
        # 최초 상장일을 기준으로 상장폐지 기록이 있다면 상장폐지 이후 market 정보를 참조하여 상장일을 만든다
        _listed_dt = _init_listed_dt.copy()
        _listed_dt = _listed_dt.rename(columns={'init_listed_dt': 'listed_dt'})
        _listed_dt['changed_reason'] = np.nan

        # 상장폐지
        _delisted = _get_data('delisted', _ticker)
        _delisted = _delisted.rename(columns={'value': 'changed_reason'})

        if not _delisted.empty:
            _delisted['delisted_dt'] = _delisted.index.values
            _delisted['changed_reason'] = _delisted['changed_reason'].map(lambda l: '[상장폐지] - ' + l)
        else:
            _delisted = _init_listed_dt.copy().rename(columns={'init_listed_dt': 'delisted_dt'})
            _delisted['delisted_dt'] = np.nan

        # 이전상장
        _market = _get_data('market', _ticker)
        _market['market'] = _market['market'].str.replace('KSE', 'KOSPI')
        _market['changed_reason'] = np.nan
        _diff_prev('market', _market, '[이전상장]')
        _market = _market[_market['market'].isin(['KOSPI', 'KOSDAQ'])]

        # 종목명
        _company_name = _get_data('company_name', _ticker)
        _company_name['changed_reason'] = np.nan
        _diff_simple('company_name', _company_name, '[종목명변동]')

        if _company_name['company_name'].map(lambda l: '스팩' in l).any():
            return None

        if _company_name.empty:
            return None

        # 상장일이 맞지않는경우가 있음. (상장일에 daily data 들이 비어있음)
        # 이런 경우는 데이터가 차있는 날짜로 상장일을 바꿔준다.
        if _company_name.index[0] != '19991228' and _company_name.index[0] != _init_listed_dt.index[0]:
            _init_listed_dt.index = [_company_name.index[0]]
            _init_listed_dt.loc[_company_name.index[0], 'init_listed_dt'] = _company_name.index[0]
            _listed_dt.index = [_company_name.index[0]]
            _listed_dt.loc[_company_name.index[0], 'listed_dt'] = _company_name.index[0]

        # _market = _market[_market.index.map(lambda l: l >= _init_listed_dt.index[0]).values]

        # 액면가
        _face_value = _get_data('face_value', _ticker)
        _face_value['changed_reason'] = np.nan

        # 발행주식수
        # _issued_shares = _get_data('issued_shares', _ticker)
        # _issued_shares['issued_shares'][_issued_shares['issued_shares'] == '0'] = np.nan
        # _issued_shares['changed_reason'] = np.nan
        # _issued_shares = _issued_shares.dropna(how='all')

        # 발행주식수가 상장폐지 이후에 기록되는경우를 merge 한다
        # for x in _delisted.index:
        #     range_e = (pd.Timestamp(x) + pd.DateOffset(weeks=1)).strftime('%Y%m%d')
        #     merge_item = _issued_shares.index[_issued_shares.index.map(lambda l: x < l < range_e)]
        #     if len(merge_item) < 1:
        #         continue
        #
        #     merge_item = merge_item[0]
        #     _issued_shares = _issued_shares.rename(index={merge_item: x})
        #     _issued_shares = _issued_shares.reset_index().drop_duplicates('std_dt', keep='last').set_index('std_dt')

        # _diff_simple('issued_shares', _issued_shares, '[발행주식수변동]')

        _diff_simple('face_value', _face_value, '[액면가변동]')

        # 통화
        _currency = _get_data('currency', _ticker)
        _currency['changed_reason'] = np.nan

        _diff_simple('currency', _currency, '[통화변동]')

        if not _delisted.dropna(how='all').empty:
            for x in _delisted.index:
                _temp = _market.index[_market.index >= x]
                if len(_temp) < 1:
                    continue

                _listed_dt.loc[_temp[0], 'listed_dt'] = _temp[0]
                _listed_dt.loc[_temp[0], 'changed_reason'] = '[재상장]'

        # data 생성
        _op_df = pd.concat([
            _isin,
            _init_listed_dt,
            _listed_dt,
            _delisted,
            _market,
            _company_name,
            _face_value,
            # _issued_shares,
            _currency
        ], axis=1)

        # 데이터 오류 수정. 최초상장일 이전날짜의 데이터는 최초 상장일로 날짜변경
        _temp_dt = _init_listed_dt['init_listed_dt'][0]
        _op_df.index = _op_df.index.rename('std_dt')
        _op_df = _op_df.reset_index()
        _op_df['std_dt'] = _op_df['std_dt'].mask(_op_df['std_dt'] < _temp_dt, _temp_dt)
        _op_df = _op_df.set_index('std_dt')

        # 중복되어있는 changed_reason column 정리
        _op_df = _op_df.groupby(_op_df.columns, axis=1).first().sort_index()
        _op_df = _op_df.dropna(how='all')

        if _op_df['company_name'].map(pd.isnull).all():
            return None

        # 최초상장일이 19991228 이후라면 첫번째 데이터의 changed_reason 을 비워준다
        if _op_df.index[0] > '19991228':
            _op_df.loc[_op_df.index[0], 'changed_reason'] = np.nan

        # 첫번째 row 가 19991228 보다 작다면 2000년 이전 상장기록이 있는 경우이기때문에 이를 19991228 로 복사해준다
        if _op_df.index[0] < '19991228':
            _op_df = _op_df.rename(index={_op_df.index[0]: '19991228'})
            # 오래된 기업 중 2000 년 1월 4일 (2000년 첫 영업일) 데이터가 상장일로 설정되어있다면 19991228로 바꿔준다
            if _op_df.loc['20000104', 'listed_dt'] == '20000104':
                _op_df.loc['20000104', 'changed_reason'] = np.nan
                _op_df.loc['20000104', 'listed_dt'] = _op_df.iloc[0]['listed_dt']
                loc = _op_df.index.get_loc('20000104')
                index = _op_df.index.tolist()
                index[loc] = '19991228'
                _op_df.index = index

        _op_df = _op_df.groupby(_op_df.index.values).ffill().reset_index().drop_duplicates(
            subset='std_dt', keep='last').set_index('std_dt')
        #
        # # 맨처음 market data 가 비어있을경우 아래껄로 채움
        # if pd.isnull(_op_df.iloc[0]['market']):
        #     _op_df.iloc[0]['market'] = _op_df.iloc[1]['market']
        #     _op_df.iloc[1]['market'] = np.nan

        _op_df = _op_df.dropna(how='all')

        # 상장폐지 데이터를 제외한 나머지는 ffill 처리
        _temp_df = _op_df[~_op_df['delisted_dt'].isnull()].copy()
        _temp_sr = _op_df['delisted_dt'].copy()
        _op_df = _op_df.ffill()
        _op_df.loc[_temp_df.index] = _temp_df
        _op_df['delisted_dt'] = _temp_sr

        _op_df['changed_dt'] = _op_df.index.values
        _op_df['end_dt'] = _op_df.index.values
        _op_df['end_dt'] = _op_df['end_dt'].shift(-1)
        _op_df['min_order'] = 1
        _op_df['trading_unit'] = 1

        _op_df['ticker'] = _ticker
        _op_df['dn_id'] = _ticker_to_dnid(_ticker)

        _op_df = _op_df[column_order]

        # 상장폐지종목 isin, company_name, init_listed_dt, listed_dt, market, currency 가 비어있는데 채운다.
        _delisted = _delisted.dropna(how='all')

        for _x, _sr in _delisted.iterrows():
            _loc = _op_df.index.get_loc(_x)
            _prev_sr = _op_df.iloc[_loc - 1]
            _op_df.loc[_x, 'isin'] = _prev_sr['isin']
            _op_df.loc[_x, 'company_name'] = _prev_sr['company_name']
            _op_df.loc[_x, 'init_listed_dt'] = _prev_sr['init_listed_dt']
            _op_df.loc[_x, 'listed_dt'] = _prev_sr['listed_dt']
            _op_df.loc[_x, 'face_value'] = _prev_sr['face_value']

            # 이전상장의 경우 pass
            if pd.isnull(_op_df.loc[_x, 'market']) or _prev_sr['market'] == _op_df.loc[_x, 'market']:
                _op_df.loc[_x, 'market'] = _prev_sr['market']

            _op_df.loc[_x, 'currency'] = _prev_sr['currency']

        # KONEX 나 K-OTC 로 최초 상장된 종목의 정보는 지워준다.
        if pd.isnull(_op_df.iloc[0]['market']):
            _op_df = _op_df[~(pd.isnull(_op_df['market']) & pd.isnull(_op_df['delisted_dt']))]

            # 첫번째 데이터 정보를 수정한다.
            if _op_df.index[0] > '20000104':
                init_listed = _op_df.index[0]
                _op_df['init_listed_dt'] = init_listed
                _op_df.loc[init_listed, 'delisted_dt'] = np.nan
                _op_df.loc[init_listed, 'listed_dt'] = init_listed
                _op_df.loc[init_listed, 'changed_reason'] = ''

        return _op_df

    history_dict = OrderedDict()

    for i, ticker in enumerate(ticker_list):
        print(f'1. {i}/{len(ticker_list)}')
        history_df = _generate_history(ticker)
        if history_df is None:
            continue
        history_dict[ticker] = history_df

    csv_file_path = './output/operation.csv'

    for i, (k, v) in enumerate(history_dict.items()):
        print(f'2. {i}/{len(history_dict)}')
        if i == 0:
            v.to_csv(csv_file_path, encoding='utf-8-sig', mode='w', index=False)
        else:
            v.to_csv(csv_file_path, encoding='utf-8-sig', mode='a', index=False, header=False)


if __name__ == '__main__':
    # 테이블 생성
    # setup_tables()
    # operation
    insert_kr_stock_operation()

    pass
