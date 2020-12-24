from dndata.common.dbsession import DBSession
from dndata.data.api.util import api_session_scope
from sqlalchemy.sql import text, bindparam
import sqlalchemy

__all__ = [
    'get_listed_company',
    'get_listed_company_detail'
]

"""
    TODO: 상장회사의 시장정보를 구분하여 가져와야함. 이게 충족되려면 이전상장 history 도 들고 있어야함. (schema 변경)
"""


def get_listed_company(standard_date: str, session: DBSession = None):
    """
    상장회사 목록
    :param standard_date: 기준일 (YYYYMMDD)
    :param session: db session (default=None)
    :return: (list)
        상장회사 목록
    """
    df = get_listed_company_detail(standard_date, session)
    return df['stock_code'].values.tolist()


def get_listed_company_detail(standard_date: str, session: DBSession = None):
    """
    상장회사 세부정보
    :param standard_date: 기준일 (YYYYMMDD)
    :param session: db session (default=None)
    :return: (dataframe)
        상장회사 세부정보
    """
    stmt = text('''
                select *
                from fnguide_company_info
                where lt_dt is not null
                  and (dlt_dt > :dlt_dt or dlt_dt is null)
            ''')

    stmt = stmt.bindparams(bindparam('dlt_dt', type_=sqlalchemy.Date))

    with api_session_scope(session) as ss:
        df = ss.fetch(stmt, {'dlt_dt': standard_date}).df()

    return df
