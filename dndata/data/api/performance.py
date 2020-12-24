from dndata.common.dbsession import DBSession
from dndata.data.api.util import api_session_scope
from sqlalchemy.sql import text, bindparam
import sqlalchemy
import pandas as pd

__all__ = [
    'get_performance_daily',
    'get_performance_periodically'
]

_item_map = {
    'per': {
        'type': 'daily',
        'field_name': 'per'
    },
    'pbr': {
        'type': 'daily',
        'field_name': 'pbr'
    },
    'pcr': {
        'type': 'daily',
        'field_name': 'pcr'
    },
    'psr': {
        'type': 'daily',
        'field_name': 'psr'
    },
    'dividend_yield': {
        'type': 'daily',
        'field_name': 'dividend_yield_ratio'
    },
    'roe': {
        'type': 'periodically',
        'field_name': 'roe'
    },
    'roa': {
        'type': 'periodically',
        'field_name': 'roa'
    },
}


def _parse_perf_select_string(search_item_list):
    daily = []
    periodic = []

    if search_item_list == 'ALL':
        # ALL 처리
        for _, v in _item_map.items():
            if v['type'] == 'daily':
                daily.append(v['field_name'])
            else:
                periodic.append(v['field_name'])

    else:
        # 개별 처리
        for x in search_item_list:
            item = _item_map.get(x, None)
            if item is None:
                continue

            if item['type'] == 'daily':
                daily.append(item['field_name'])
            else:
                periodic.append(item['field_name'])

    if not daily:
        daily = None
    else:
        daily = ','.join(daily)

    if not periodic:
        periodic = None
    else:
        periodic = ','.join(periodic)

    return daily, periodic


def _pivot_perf_data(src_df, mode, lag, cut):
    """
    performance data 정리
    :param src_df: raw data
    :param mode: daily, periodically
    :param lag: lag
    :param cut: cut range
    :return: (dict)
        데이터 정리하여 dict[dataframe] 형태로 반환
    """
    result_dict = {}
    cut_s = pd.Timestamp(cut[0])
    cut_e = pd.Timestamp(cut[1])

    for k, v in _item_map.items():
        if v['type'] == mode:
            if src_df is None:
                continue
            if v['field_name'] in src_df.columns:
                df = src_df.pivot(index='std_dt', columns='stock_code', values=v['field_name'])
                result_dict[k] = df.shift(lag)[cut_s:cut_e].astype(float)

    return result_dict


def get_performance_daily(symbol_list: list, start_date: str, end_date: str,
                          search_item_list='ALL', lag=0,
                          session: DBSession = None):
    """
    종목 performance 정보 조회 (일단위)
    :param symbol_list: (list)
        조회할 업체 코드 목록 (stock_code list)
    :param start_date: (str)
        조회 시작일
    :param end_date: (str)
        조회 마지막일
    :param search_item_list: (str or list)
        조회할 정보. 복수 입력가능. (아래에 정의. D: Daily, Y: Yearly)
        ALL: 모두다
        per(D), pbr(D), pcr(D), psr(D), dividend_yield(D)
    :param lag: (int)
        lag. 1이면 하루전데이터
    :param session: (DBSession)
        db session (default=None)
    :return: (dataframe)
        조회 결과
    """

    daily_string, _ = _parse_perf_select_string(search_item_list)

    if daily_string is None:
        return {}

    with api_session_scope(session) as ss:
        stmt = text(f'''
                        select std_dt, stock_code, {daily_string}
                        from fnguide_performance_daily
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

        df = ss.fetch(stmt, {'start_date': _start,
                             'end_date': _end,
                             'symbol_list': symbol_list}).df()

    return _pivot_perf_data(df, 'daily', lag, [start_date, end_date])


def get_performance_periodically(symbol_list: list, start_date: str, end_date: str,
                                 freq='y', search_item_list='ALL', lag=0, session: DBSession = None):
    """
    종목 performance 정보 조회 (주기적인)
    :param symbol_list: (list)
        조회할 업체 코드 목록 (stock_code list)
    :param start_date: (str)
        조회 시작일
    :param end_date: (str)
        조회 마지막일
    :param freq: (str)
        주기 (y: 연, q: 분기)
    :param search_item_list: (str or list)
        조회할 정보. 복수 입력가능. (아래에 정의. D: Daily, Y: Yearly)
        ALL: 모두다
        roa, roe
    :param lag: (int)
        lag. 1이면 1년전 데이터
    :param session: (DBSession)
        db session (default=None)
    :return: (dataframe)
        조회 결과
    """

    _, periodic_string = _parse_perf_select_string(search_item_list)

    if periodic_string is None:
        return {}

    with api_session_scope(session) as ss:
        stmt = text(f'''
                        select std_dt, stock_code, freq, {periodic_string}
                        from fnguide_performance_period
                        where stock_code in :symbol_list and
                          std_dt between :start_date and :end_date
                    ''')

        stmt = stmt.bindparams(bindparam('symbol_list', expanding=True),
                               bindparam('start_date', type_=sqlalchemy.Date),
                               bindparam('end_date', type_=sqlalchemy.Date))

        _start = start_date
        _end = end_date

        offset_param = {}
        if freq == 'q':
            offset_param['months'] = abs(lag / 3) + 1
        elif freq == 'y':
            offset_param['years'] = abs(lag) + 1

        if lag > 0:
            _start = (pd.Timestamp(start_date) - pd.DateOffset(**offset_param)).strftime('%Y%m%d')
        elif lag < 0:
            _end = (pd.Timestamp(end_date) + pd.DateOffset(**offset_param)).strftime('%Y%m%d')

        df = ss.fetch(stmt, {'start_date': _start,
                             'end_date': _end,
                             'symbol_list': symbol_list}).df()

    return _pivot_perf_data(df, 'periodically', lag, [start_date, end_date])
