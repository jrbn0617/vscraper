import numpy as np
import traceback
import logging
from .exception import APIError
from .util import api_response_json
from flask import request

logger = logging.getLogger('system')


def error_handling():
    """
    api 이용시 오류를 catch 하여 해당내용 response
    usage:
        @test.api.route('/v1/api/test')
        class TestApiClass(APIRoot):
            @test.api.expect(model_test)
            @api_wrapper()
            def post(self):
                data_dict = request.get_json(force=True)
                ...
    """

    def wrapper(func):
        def decorator(*args, **kwargs):
            logger.info(f'{request.path} - {request.json}')

            try:
                result = func(*args, **kwargs)
            except APIError as e:
                error_message = str(e)
                logger.error(f'{request.path} - {error_message}')
                return api_response_json(dict(result='failure',
                                              message=error_message))
            except Exception:
                error_message = traceback.format_exc(chain=False)
                logger.error(f'{request.path} - {error_message}')
                return api_response_json(dict(result='failure',
                                              message=error_message))

            return result

        return decorator

    return wrapper


def raise_handling(cls):
    """
    function 내 에러를 input cls 로 변환하여 반환
    :param cls: 변환하여 반환 될 exception class
    """

    def wrapper(func):
        def decorator(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
            except cls as e:
                e.function_name = func.__name__
                raise e
            except Exception:
                traceback.print_exc(chain=False)
                raise cls(function_name=func.__name__)
            return result

        return decorator

    return wrapper
