from .company import *
from .performance import *
from .trading import *


def initialize(uri):
    """
    api 초기화
    :param uri: db uri
    """
    from dndata.common.dbsession import DBAdaptor
    # system 종료하면 자동으로 해제됨
    setattr(initialize, 'adaptor', DBAdaptor(uri))


