from flask import Response
import json
import decimal
import datetime
import logging
import numpy as np
from .dbsession import DBSession


# count 개씩 indexing
def iter_count(indexes: list, count: int):
    a = indexes[::count] + [None]
    return list(zip(a[:-1], a[1:]))


def _json_serializer(value):
    if isinstance(value, bytes):
        return bool.from_bytes(value, 'big')
    elif isinstance(value, decimal.Decimal):
        return float(value)
    elif isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    elif isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    else:
        return value


def api_response_json(data, status=200):
    """
    json 형식으로 api 응답
    :param data: (dict)
    :param status: response status
    :return:
        response 결과
    """
    return Response(json.dumps(data, ensure_ascii=False, default=_json_serializer).replace("\"nan\"", 'null'),
                    status=status,
                    mimetype='application/json')


def _db_safe_upload_values_count(allowed_packet_size, value_string):
    value_size = len(value_string.encode('utf-8'))
    return int(allowed_packet_size / value_size * 0.5)


def db_big_update(session: DBSession, stmt, input_args):
    max_size = session.fetch("SHOW VARIABLES LIKE 'max_allowed_packet'").scalar(1)

    window_size = _db_safe_upload_values_count(int(max_size), str(input_args[0]))
    iter_list = iter_count(list(range(len(input_args))), window_size)

    result_row_count = 0
    for i, (x, y) in enumerate(iter_list):
        result_row_count += session.update(stmt, input_args[x:y])

    return result_row_count
