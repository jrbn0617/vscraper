import pandas as pd
from dndata import ROOT_DIR
from sqlalchemy.sql import text, bindparam
from dndata.common.util import db_big_update


class DBUploadInterfaceManual:
    @property
    def filenames(self):
        return NotImplementedError

    @property
    def tablename(self):
        return NotImplementedError

    @property
    def column_map(self):
        return NotImplementedError

    @property
    def bind_info(self):
        return NotImplementedError

    def read_file(self):
        data_dict = {}
        for x in self.filenames:
            df = pd.read_excel(f'{ROOT_DIR}/resource/{x}', sheet_name=0, header=5)
            df = df.where(pd.notnull(df), None)
            data_dict[x] = df

        return data_dict

    def cleansing(self, data_dict):
        """
        input 의 data_dict 를 이용하여 해당 테이블에 적재할 하나의 dataframe 을 만든다.
        :param data_dict: file 에 저장되어있는 raw data
        :return:
            적재할 dataframe
        """
        return NotImplementedError

    def upload(self, session):
        """
        파일에 저장되어있는 데이터를 읽어 db 에 업로드 한다.
        :param session: db session 객체
        :return:
        """
        data_dict = self.read_file()
        data_df = self.cleansing(data_dict)

        input_args = data_df.to_dict(orient='records')
        bind_params = [bindparam(v['name'], type_=v['bind_type']) for k, v in self.bind_info.items()]

        columns_str = ','.join(data_df.columns)
        params_str = ','.join([f':{x}' for x in data_df.columns])

        stmt = text(f'insert into {self.tablename} ({columns_str}) values ({params_str})')
        stmt = stmt.bindparams(*bind_params)

        return db_big_update(session, stmt, input_args)
