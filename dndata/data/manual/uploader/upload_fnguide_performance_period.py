from dndata.common.types import *
from dndata.data.manual.uploader import DBUploadInterfaceManual
import sqlalchemy
import pandas as pd
import numpy as np


class UploadPerformancePeriod(DBUploadInterfaceManual):
    @property
    def file_info(self):
        return [
            {
                'name': 'roa.xlsx',
                'header': 8,
                'sheet_name': 0,
            },
            {
                'name': 'roe.xlsx',
                'header': 8,
                'sheet_name': 0
            },
        ]

    @property
    def tablename(self):
        return 'fnguide_performance_period'

    @property
    def bind_info(self):
        return {
            'std_dt': {
                'name': 'std_dt',
                'bind_type': sqlalchemy.Date
            },
            'stock_code': {
                'name': 'stock_code',
                'bind_type': sqlalchemy.String
            },
            'freq': {
                'name': 'freq',
                'bind_type': sqlalchemy.String
            },
            'roa': {
                'name': 'roa',
                'bind_type': SafeFloat
            },
            'roe': {
                'name': 'roe',
                'bind_type': SafeFloat
            }
        }

    def cleansing(self, data_dict):
        # index: 날짜, columns: 업체코드 형태로 만든다
        for k, v in data_dict.items():
            # skip rows
            df = v.iloc[4:]
            df = df.set_index(df.columns[0])
            df = df[df.columns[1:]].reset_index()
            df = df.rename(columns={'Unnamed: 0': 'std_dt'})
            df['std_dt'] = df['std_dt'].map(lambda l: str(l) + '1231')
            df = df.set_index('std_dt')
            data_dict[k] = df

        # 추가 cleansing 작업
        df = pd.concat([data_dict['roa.xlsx'].stack(),
                        data_dict['roe.xlsx'].stack()],
                       axis=1)

        # numeric 으로 표현이 되지않는 값은 None 으로 치환
        df = df.applymap(lambda l: pd.to_numeric(l, errors='coerce')).astype(float).replace([np.nan], [None])
        df = df.reset_index()

        # 순서주의!
        df.columns = ['std_dt', 'stock_code', 'roa', 'roe']
        df['freq'] = 'y'

        return df


if __name__ == '__main__':
    from dndata.common.dbsession import DBAdaptor, session_scope

    uri = 'mysql+pymysql://stock_dev:dlNblDJ35[v_20201215@richgoweb.cluster-czdphhgdzbon.ap-northeast-2.rds.amazonaws.com:3306/stock'
    adaptor = DBAdaptor(uri)

    with session_scope(adaptor, commit=True) as _session:
        rowcount = UploadPerformancePeriod().upload(_session)

    print(rowcount)
