import numpy as np
import traceback
from .exception import APIError
from affogato import response_json, logger
from flask import request


def args_type_convert(func):
    def decorator(*args, **kwargs):
        type_mapper = {
            np.int64: int,
        }
        converted_args = []
        converted_kwargs = {}
        for i, value in enumerate(args):
            for src_type, target_type in type_mapper.items():
                if isinstance(value, src_type):
                    converted_args.append(target_type(value))
                else:
                    converted_args.append(value)

        for k, value in kwargs.items():
            for src_type, target_type in type_mapper.items():
                if isinstance(value, src_type):
                    converted_kwargs[k] = target_type(value)
                else:
                    converted_kwargs[k] = value

        return func(*converted_args, **converted_kwargs)

    return decorator


def api_wrapper():
    def wrapper(func):
        def decorator(*args, **kwargs):
            logger.info(f'{request.path} - {request.json}')

            try:
                result = func(*args, **kwargs)
            except APIError as e:
                error_message = str(e)
                logger.error(f'{request.path} - {error_message}')
                return response_json(dict(result='failure',
                                          message=error_message))
            except Exception:
                error_message = traceback.format_exc(chain=False)
                logger.error(f'{request.path} - {error_message}')
                return response_json(dict(result='failure',
                                          message=error_message))

            return result
        return decorator
    return wrapper


