from dndata.common.dbsession import DBSession
from dndata.data.api.util import api_session_scope
from sqlalchemy.sql import text, bindparam
import sqlalchemy
import pandas as pd

__all__ = [
    'get_trading'
]

_item_map = {
    'adj_close': {
        'type': 'daily',
        'field_name': 'adj_close'
    },
    'vol_52w': {
        'type': 'daily',
        'field_name': 'vol_52w'
    }
}


def _pivot_trading_data(src_df, lag, cut):
    """
    trading data 정리
    :param src_df: raw data
    :param lag: lag
    :param cut: cut range
    :return: (dict)
        데이터 정리하여 dict[dataframe] 형태로 반환
    """
    result_dict = {}
    cut_s = pd.Timestamp(cut[0])
    cut_e = pd.Timestamp(cut[1])

    for k, v in _item_map.items():
        if src_df is None:
            continue
        if v['field_name'] in src_df.columns:
            df = src_df.pivot(index='std_dt', columns='stock_code', values=v['field_name'])
            result_dict[k] = df.shift(lag)[cut_s:cut_e].astype(float)

    return result_dict


def _safe_search_item(items):
    if items == 'ALL':
        result_items = [v['field_name'] for _, v in _item_map.items()]
    else:
        result_items = []
        for k, v in _item_map.items():
            if k in items:
                result_items.append(v['field_name'])

    return result_items


def get_trading(symbol_list, start_date, end_date,
                search_item_list='ALL', lag=0, session: DBSession = None):
    """
    종목 trading 정보 조회 (일별)
    :param symbol_list: (list)
        조회할 업체 코드 목록 (stock_code list)
    :param start_date: (str)
        조회 시작일
    :param end_date: (str)
        조회 마지막일
    :param search_item_list: (str or list)
        조회할 정보. 복수 입력가능. (아래에 정의. D: Daily, Y: Yearly)
        ALL: 모두다
        adj_close, vol_52w
    :param lag: (int)
        lag. 1이면 1년전 데이터
    :param session: (DBSession)
        db session (default=None)
    :return: (dataframe)
        조회 결과
    """

    search_item_list = _safe_search_item(search_item_list)
    search_string = ','.join(search_item_list)

    stmt = text(f'''
                    select std_dt, stock_code, {search_string}
                    from fnguide_trading
                    where stock_code in :symbol_list and
                      std_dt between :start_date and :end_date
                ''')

    stmt = stmt.bindparams(bindparam('symbol_list', expanding=True),
                           bindparam('start_date', type_=sqlalchemy.Date),
                           bindparam('end_date', type_=sqlalchemy.Date))

    _start = start_date
    _end = end_date

    weeks = abs(lag / 7 + 1)

    if lag > 0:
        _start = (pd.Timestamp(start_date) - pd.DateOffset(weeks=weeks)).strftime('%Y%m%d')
    elif lag < 0:
        _end = (pd.Timestamp(end_date) + pd.DateOffset(weeks=weeks)).strftime('%Y%m%d')

    with api_session_scope(session) as ss:
        df = ss.fetch(stmt, {'start_date': _start,
                             'end_date': _end,
                             'symbol_list': symbol_list}).df()

    return _pivot_trading_data(df, lag, [start_date, end_date])
