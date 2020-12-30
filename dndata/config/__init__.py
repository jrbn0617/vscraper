import os
from os import environ


class Config(object):
    SECRET_KEY = 'secret_key'
    DATABASE_URI = 'sqlite:///:memory:?charset=utf8'


class ProductionConfig(Config):
    DEBUG = False

    SESSION_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_DURATION = 3600

    DATABASE_URI = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(
        environ.get('DB_USER'),
        environ.get('DB_PASSWORD'),
        environ.get('DB_HOST'),
        environ.get('DB_PORT'),
        environ.get('DB_NAME')
    )


class DebugConfig(Config):
    DEBUG = True


# DEBUG, PRODUCTION
dndata_config = {
    'PRODUCTION': ProductionConfig,
    'DEBUG': DebugConfig
}[environ.get('PROJECT_ENV', 'DEBUG')]
