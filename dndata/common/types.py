import sqlalchemy
from sqlalchemy import types
import json
import numpy as np

__all__ = [
    'SafeInteger',
    'SafeJSON',
    'SafeFloat',
]


class SafeInteger(types.TypeDecorator):
    impl = types.Integer

    @property
    def python_type(self):
        return int

    def process_literal_param(self, value, dialect):
        return int(value)

    def process_bind_param(self, value, dialect):
        if value is None:
            return 'NULL'

        return int(value)

    def process_result_value(self, value, dialect):
        return int(value)


class SafeFloat(types.TypeDecorator):
    impl = types.Float

    @property
    def python_type(self):
        return float

    def process_literal_param(self, value, dialect):
        return float(round(value, 10))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None

        return float(round(value, 10))

    def process_result_value(self, value, dialect):
        return float(round(value, 10))


class SafeJSON(types.TypeDecorator):
    impl = sqlalchemy.String

    @property
    def python_type(self):
        return dict

    def process_literal_param(self, value, dialect):
        return value

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)

        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value
