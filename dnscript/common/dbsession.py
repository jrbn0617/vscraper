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
    'Session',
    'FetchResult',
    'DBAdaptor',
    'session_scope'
]


class FetchResult:
    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def df(self):
        return pd.DataFrame(columns=self.columns, data=self.data)

    def scalar(self):
        return self.data[0][0]

    def list(self):
        return list(*zip(*self.data))

    def native(self):
        return self.columns, self.data


class DBAdaptor:
    engine = None

    def __init__(self, info, pool_recycle=1200, echo=False):
        if isinstance(info, dict):
            info = self._generate_uri(**info)

        self.uri = info
        self.pool_recycle = pool_recycle
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
        if self.engine:
            if self.echo:
                logger.info(f"DBAdaptor destroy: {self.display_uri}")

            self.Session.remove()
            self.Session = None
            self.engine.dispose()
            self.engine = None


class Session:
    _session = None

    def __init__(self, adaptor: DBAdaptor):
        self._session = adaptor.Session()

    def fetch(self, query, *args, **kwargs):
        result = self._session.execute(query, *args, **kwargs)
        fetch_data = result.fetchall()
        keys = result.keys()
        result.close()
        return FetchResult(keys, fetch_data)

    def update(self, query, *args, **kwargs):
        result = self._session.execute(query, *args, **kwargs)
        rowcount = result.rowcount
        result.close()
        return rowcount

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    def close(self):
        self._session.close()

    def begin(self):
        return self._session.begin(subtransactions=True)

    @property
    def session(self):
        return self


class SessionManager:
    def __init__(self, *adaptors: List[DBAdaptor]):
        self._sessions = [Session(x) for x in adaptors]

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
def session_scope(*db_adaptors: DBAdaptor, commit=True) -> ContextManager[Session]:
    if len(db_adaptors) > 1:
        session_obj = SessionManager(*db_adaptors)
    else:
        session_obj = Session(*db_adaptors)

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
