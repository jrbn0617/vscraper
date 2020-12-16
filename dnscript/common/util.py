from flask import Response
import json
import decimal
import datetime
import logging
import numpy as np


def json_serializer(value):
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


def response_json(data, status=200):
    return Response(json.dumps(data, ensure_ascii=False, default=json_serializer).replace("\"nan\"", 'null'),
                    status=status,
                    mimetype='application/json')
