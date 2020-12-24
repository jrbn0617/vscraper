from contextlib import contextmanager
from typing import ContextManager
from dndata.common.dbsession import DBSession

__all__ = [
    'api_session_scope',
]


def get_db_adaptor():
    import dndata.data.api as api
    """
    initialize 시 생성한 db adaptor 접근
    """
    return getattr(api.initialize, 'adaptor')


@contextmanager
def api_session_scope(db_session) -> ContextManager[DBSession]:
    """
    context manager 를 사용한 session 관리. readonly
    usage:
        with session_scope(adaptor_object) as _session:
            _session.fetch(...)
    """
    if db_session is None:
        adaptor = get_db_adaptor()
        db_session = DBSession(adaptor)

    try:
        yield db_session.session
    finally:
        db_session.close()
