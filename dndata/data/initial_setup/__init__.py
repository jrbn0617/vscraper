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


def insert_kr_stock_operation():
    # container 생성
    column_order = [
        'dn_id',
        'ticker',
        'isin',
        'company_name',
        'init_listed_dt',
        'listed_dt',
        'changed_dt',
        'end_dt',
        'delisted_dt',
        'changed_reason',
        'issued_shares',
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

        # 이전상장
        _market = _get_data('market', _ticker)
        _market['market'] = _market['market'].str.replace('KSE', 'KOSPI')
        _market['changed_reason'] = np.nan
        _diff_prev('market', _market, '[이전상장]')

        # 액면가
        _face_value = _get_data('face_value', _ticker)
        _face_value['changed_reason'] = np.nan

        # 발행주식수
        _issued_shares = _get_data('issued_shares', _ticker)
        _issued_shares['issued_shares'][_issued_shares['issued_shares'] == '0'] = np.nan
        _issued_shares['changed_reason'] = np.nan

        _diff_simple('issued_shares', _issued_shares, '[발행주식수변동]')

        _diff_simple('face_value', _face_value, '[액면가변동]')

        # 통화
        _currency = _get_data('currency', _ticker)
        _currency['changed_reason'] = np.nan

        _diff_simple('currency', _currency, '[통화변동]')

        if not _delisted.dropna(how='all').empty:
            for x in _delisted.index:
                _temp = _market.index[_market.index > x]
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
            _company_name,
            _market,
            _face_value,
            _issued_shares,
            _currency
        ], axis=1)

        # 데이터 오류 수정. 최초상장일 이전날짜의 데이터는 최초 상장일로 날짜변경
        _temp_dt = _init_listed_dt['init_listed_dt'][0]
        _op_df = _op_df.reset_index()
        _op_df['index'] = _op_df['index'].mask(_op_df['index'] < _temp_dt, _temp_dt)
        _op_df = _op_df.set_index('index')

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
            subset='index', keep='last').set_index('index')

        # 맨처음 market data 가 비어있을경우 아래껄로 채움
        if pd.isnull(_op_df.iloc[0]['market']):
            _op_df.iloc[0]['market'] = _op_df.iloc[1]['market']
            _op_df.iloc[1]['market'] = np.nan

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

        return _op_df[column_order]

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

    # 종목별로 순회하면서 데이터를 채운다


def insert_kr_stock_operation_():
    # dn_id          varchar(20)                        not null comment '자산 고유 ID(변하지않음)',
    # ticker         varchar(20)                        not null comment '자산 ticker',
    # isin           varchar(20)                        not null comment '국제표준코드',
    # asset_name     varchar(100)                       not null comment '자산 이름',
    # init_listed_dt date                               not null comment '최초 상장일 (최초 등록일)',
    # listed_dt      date                               not null comment '상장일 (등록일)',
    # changed_dt     date                               not null comment '데이터 변경일',
    # delisted_dt    date                               null comment '상장폐지일',
    # changed_reason varchar(500)                       null comment '데이터 변경 사유',
    # issued_shares  bigint                             null comment '발행주수',
    # face_value     float                              null comment '액면가',
    # market         varchar(20)                        null comment '시장 (KOSPI, KOSDAQ ...)',
    # currency       varchar(10)                        null comment '통화 (KRW, USD ...)',
    # min_order      float    default '1'               null comment '최저 매수 주문금액 (통화)',
    # trading_unit   float    default '1'               null comment '최소 매매 단위 (주)',
    # created_at     datetime default CURRENT_TIMESTAMP null,
    # updated_at     datetime default CURRENT_TIMESTAMP null,

    def _ticker_to_dnid(_sr):
        return _sr.apply(lambda l: 'SKR-' + l)

    # isin code 를 기준으로 universe 구성
    operation_df = pd.read_csv('./resource/isin.csv', index_col=0)
    operation_df = operation_df.reset_index()
    operation_df.columns = ['ticker', 'isin']
    # dn_id 는 앞에 SKR- 를 붙여준다. (S = Stock, KR = 대한민국)
    operation_df['dn_id'] = _ticker_to_dnid(operation_df['ticker'])

    # 추가할 데이터 형태를 잡아준다
    operation_df['asset_name'] = None
    operation_df['listed_dt'] = None
    operation_df['changed_dt'] = None
    operation_df['delisted_dt'] = None
    operation_df['changed_reason'] = None
    operation_df['issued_shares'] = None
    operation_df['face_value'] = None
    operation_df['market'] = None
    operation_df['currency'] = None

    # 최초 상장일 추가
    temp_df = pd.read_csv('./resource/initial_listed_date.csv')
    temp_df.columns = ['ticker', 'init_listed_dt']
    temp_df = temp_df.dropna()
    temp_df['init_listed_dt'] = temp_df['init_listed_dt'].astype(int).astype(str)

    operation_df = operation_df.merge(temp_df.dropna(), on='ticker', how='left')

    # 최초 상장일이 없는 종목 제거
    operation_df = operation_df.loc[operation_df['init_listed_dt'].dropna().index]

    # # 최초 상장일이 데이터 시작일보다 큰경우 처리
    # temp_df = operation_df.query('init_listed_dt > changed_dt')
    # operation_df.loc[temp_df.index, 'changed_dt'] = operation_df.loc[temp_df.index, 'init_listed_dt']

    # 상장폐지 history 적용
    temp_df = pd.read_csv('resource/delisted.csv', index_col=0)

    for _, sr in temp_df.iterrows():
        _df = operation_df.query(f'ticker == "{sr["symbol"]}"')

        # 해당 종목이 없으면 pass
        if _df.empty:
            continue

        _sr = _df.sort_values('changed_dt').iloc[-1]

        # operation_df.query(f'symbol == @sr["symbol"]')
        kkk = 0

    kkk = 0


if __name__ == '__main__':
    # 테이블 생성
    # setup_tables()
    # operation
    insert_kr_stock_operation()

    pass
