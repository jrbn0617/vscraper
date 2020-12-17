import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import ArgumentError
import atexit
import re
import logging
from contextlib import contextmanager
from typing import ContextManager, List

logger = logging.getLogger('system')

__all__ = [
    'DBSession',
    'FetchResult',
    'DBAdaptor',
    'session_scope'
]


class FetchResult:
    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def df(self):
        """
        :return: fetch 결과를 dataframe 형태로 반환
        """
        return pd.DataFrame(columns=self.columns, data=self.data)

    def scalar(self, index=0):
        """
        :return: fetch 결과 data 의 index 번째 값을 scalar 형태로 반환
        """
        return self.data[0][index]

    def list(self):
        """
        :return: fetch 결과 data 를 list 형태로 반환
        """
        return list(*zip(*self.data))

    def native(self):
        """
        :return: fetch 결과 그대로 반환
        """
        return self.columns, self.data


class DBAdaptor:
    """
    DB Adaptor 관리객체. aqlalchemy engine 객체와 sessionmaker 객체를 관리한다.
    """
    engine = None

    def __init__(self, info, echo=False):
        """
        :param info: db 정보. uri 형태와 dict 형태 두가지 가능.
        :param echo: system log 를 기록할지 여부
        """
        if isinstance(info, dict):
            info = self._generate_uri(**info)

        self.uri = info
        self.display_uri = re.sub(r'[^:]*[@$]', '******@', self.uri)
        self.echo = echo
        self.Session = None

        try:
            if echo:
                logger.info(f"DBAdaptor create: {self.display_uri}")
            self.engine = sqlalchemy.create_engine(info,
                                                   pool_pre_ping=True,
                                                   pool_recycle=500,
                                                   max_overflow=20)
            self.Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))

        except KeyError as e:
            raise AttributeError("Need to proper key %s" % str(e))
        except ArgumentError:
            raise ValueError("Incorrect arguments for SQL Manager")

        atexit.register(self.close)

    @staticmethod
    def _generate_uri(**info):
        db_connect_url = '{db_type}://{user}:{password}@{host}:{port}/{database}'
        return db_connect_url.format(**info)

    def close(self):
        """
        기본적으로 atexit 모듈을 통해 자동으로 호출되지만, 명시적으로 해제 하는것을 추천. (메모리 해제순서가 보장되지않음)
        """
        if self.engine:
            if self.echo:
                logger.info(f"DBAdaptor destroy: {self.display_uri}")

            self.Session.remove()
            self.Session = None
            self.engine.dispose()
            self.engine = None


class DBSession:
    """
    DB Session 관리 객체
    fetch, update, commit, rollback 등을 한다.
    * 객체사용이 끝나면 close() 를 호출해야한다.
    """
    _session = None

    def __init__(self, adaptor: DBAdaptor):
        self._session = adaptor.Session()

    def fetch(self, stmt, *args, **kwargs):
        """
        get data
        :param stmt: statement 객체 (or raw query)
        :param args: arguments
        :param kwargs: keyword arguments
        :return:
        """
        result = self._session.execute(stmt, *args, **kwargs)
        fetch_data = result.fetchall()
        keys = result.keys()
        result.close()
        return FetchResult(keys, fetch_data)

    def update(self, stmt, *args, **kwargs):
        """
        insert, update data
        :param stmt: statement 객체 (or raw query)
        :param args: arguments
        :param kwargs: keyword arguments
        :return:
        """
        result = self._session.execute(stmt, *args, **kwargs)
        rowcount = result.rowcount
        result.close()
        return rowcount

    def commit(self):
        """
        session commit (update 이용시 사용가능)
        """
        self._session.commit()

    def rollback(self):
        """
        session 을 이용해 update 된 항목 rollback
        """
        self._session.rollback()

    def close(self):
        """
        session 객체 해제
        """
        self._session.close()

    def begin(self):
        return self._session.begin(subtransactions=True)

    @property
    def session(self):
        return self


class DBSessionManager:
    """
    여러개의 session, adaptor 를 한번에 관리하기위함
    """
    def __init__(self, *adaptors: List[DBAdaptor]):
        self._sessions = [DBSession(x) for x in adaptors]

    def commit(self):
        [x.commit() for x in self._sessions]

    def rollback(self):
        [x.rollback() for x in self._sessions]

    def close(self):
        [x.close() for x in self._sessions]

    def begin(self):
        [x.begin() for x in self._sessions]

    @property
    def session(self):
        return self._sessions


@contextmanager
def session_scope(*db_adaptors: DBAdaptor, commit=True) -> ContextManager[DBSession]:
    """
    context manager 를 사용한 session 관리
    usage:
        with session_scope(adaptor_object) as _session:
            _session.fetch(...)
    """
    if len(db_adaptors) > 1:
        session_obj = DBSessionManager(*db_adaptors)
    else:
        session_obj = DBSession(*db_adaptors)

    try:
        yield session_obj.session
        if commit:
            session_obj.commit()
        else:
            session_obj.rollback()
    except:
        session_obj.rollback()
        raise
    finally:
        session_obj.close()
