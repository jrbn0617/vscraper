from dndata.common.types import *
from dndata.data.manual.uploader import DBUploadInterfaceManual
import sqlalchemy


class UploadCompanyInfo(DBUploadInterfaceManual):
    @property
    def file_info(self):
        return [
            {
                'name': 'company.xlsx',
                'header': 5,
                'sheet_name': 0
            }
        ]

    @property
    def tablename(self):
        return 'fnguide_company_info'

    @property
    def bind_info(self):
        return {
            'Symbol': {
                'name': 'stock_code',
                'bind_type': sqlalchemy.String
            },
            'Name': {
                'name': 'codename',
                'bind_type': sqlalchemy.String
            },
            '국제표준코드': {
                'name': 'isin',
                'bind_type': sqlalchemy.String
            },
            '종목명 (영문)': {
                'name': 'codename_en',
                'bind_type': sqlalchemy.String
            },
            '설립일': {
                'name': 'est_dt',
                'bind_type': sqlalchemy.Date
            },
            'HomePage URL': {
                'name': 'hm_url',
                'bind_type': sqlalchemy.String
            },
            '상장(등록)일자': {
                'name': 'lt_dt',  # listing date
                'bind_type': sqlalchemy.Date
            },
            '상장폐지일자': {
                'name': 'dlt_dt',  # delisting date
                'bind_type': sqlalchemy.Date
            },
            '시장이전일': {
                'name': 'tlt_dt',  # transfer of listing date
                'bind_type': sqlalchemy.Date
            },
            '시장이전내용': {
                'name': 'tlt_desc',  # description of listing transfer
                'bind_type': sqlalchemy.String
            },
            '거래소 업종구분': {
                'name': 'ex_ind',
                'bind_type': sqlalchemy.String
            },
            '한국표준산업분류10차(세세분류)': {
                'name': 'kr_ind_5',
                'bind_type': sqlalchemy.String
            },
            '한국표준산업분류코드10차(세세분류)': {
                'name': 'kr_ind_5_cd',
                'bind_type': SafeInteger
            },
            '한국표준산업분류10차(세분류)': {
                'name': 'kr_ind_4',
                'bind_type': sqlalchemy.String
            },
            '한국표준산업분류코드10차(세분류)': {
                'name': 'kr_ind_4_cd',
                'bind_type': SafeInteger
            },
            '한국표준산업분류10차(소분류)': {
                'name': 'kr_ind_3',
                'bind_type': sqlalchemy.String
            },
            '한국표준산업분류코드10차(소분류)': {
                'name': 'kr_ind_3_cd',
                'bind_type': SafeInteger
            },
            '한국표준산업분류10차(중분류)': {
                'name': 'kr_ind_2',
                'bind_type': sqlalchemy.String
            },
            '한국표준산업분류코드10차(중분류)': {
                'name': 'kr_ind_2_cd',
                'bind_type': SafeInteger
            },
            '한국표준산업분류10차(대분류)': {
                'name': 'kr_ind_1',
                'bind_type': sqlalchemy.String
            },
            '한국표준산업분류코드10차(대분류)': {
                'name': 'kr_ind_1_cd',
                'bind_type': sqlalchemy.String
            },
            'MKF500 편입여부': {
                'name': 'mkf500_yn',
                'bind_type': sqlalchemy.String
            }
        }

    def cleansing(self, data_dict):
        filename = self.file_info[0]['name']
        data_df = data_dict[filename]
        data_df = data_df[list(self.bind_info.keys())]
        return data_df.rename(columns=lambda l: self.bind_info[l]['name'])


if __name__ == '__main__':
    from dndata.common.dbsession import DBAdaptor, session_scope

    uri = 'mysql+pymysql://stock_dev:dlNblDJ35[v_20201215@richgoweb.cluster-czdphhgdzbon.ap-northeast-2.rds.amazonaws.com:3306/stock'
    adaptor = DBAdaptor(uri)

    with session_scope(adaptor, commit=True) as _session:
        rowcount = UploadCompanyInfo().upload(_session)

    print(rowcount)


