import sqlalchemy
from dndata.common.dbsession import DBAdaptor, session_scope
import pandas as pd

db_uri = 'mysql+pymysql://root:1234@localhost:3306/dnstock'
db_adaptor = DBAdaptor(db_uri)


def parse_sql_script(script_path):
    with open(script_path, 'rb') as fp:
        sql_text = fp.read().decode("UTF-8")

    sql_text = sql_text.split('\n')
    container = []
    sql_command = ''

    # Iterate over all lines in the sql file
    for line in sql_text:
        # Ignore commented lines
        if not (line.startswith('--') or line.startswith('#')) and line.strip('\n'):
            # Append line to the command string
            sql_command += line.strip('\n')

            # If the command string ends with ';', it is a full statement
            if sql_command.endswith(';'):
                container.append(sql_command)
                sql_command = ''

    return container


def setup_tables():
    sql_command_list = parse_sql_script('./resource/DDL_mysql.sql')

    with session_scope(db_adaptor, commit=True) as session:
        for x in sql_command_list:
            stmt = sqlalchemy.text(x)
            session.update(stmt)


def insert_operation():
    # dn_id          varchar(20)                        not null comment '자산 고유 ID(변하지않음)',
    # ticker         varchar(20)                        not null comment '자산 ticker',
    # isin           varchar(20)                        not null comment '국제표준코드',
    # asset_name     varchar(100)                       not null comment '자산 이름',
    # init_listed_dt date                               not null comment '최초 상장일 (최초 등록일)',
    # listed_dt      date                               not null comment '상장일 (등록일)',
    # changed_dt     date                               not null comment '데이터 변경일',
    # delisted_dt    date                               null comment '상장폐지일',
    # changed_reason varchar(500)                       null comment '데이터 변경 사유',
    # issued_shares  bigint                             null comment '발행주수',
    # face_value     float                              null comment '액면가',
    # market         varchar(20)                        null comment '시장 (KOSPI, KOSDAQ ...)',
    # currency       varchar(10)                        null comment '통화 (KRW, USD ...)',
    # min_order      float    default '1'               null comment '최저 매수 주문금액 (통화)',
    # trading_unit   float    default '1'               null comment '최소 매매 단위 (주)',
    # created_at     datetime default CURRENT_TIMESTAMP null,
    # updated_at     datetime default CURRENT_TIMESTAMP null,
    pd.read_csv('./resource/changed_name.csv')
    pass


if __name__ == '__main__':
    # 테이블 생성
    # setup_tables()
    # operation
    insert_operation()

    pass
