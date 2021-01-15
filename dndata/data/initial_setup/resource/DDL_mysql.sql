create table operation
(
    dn_id          varchar(20)                        not null comment '자산 고유 ID(변하지않음)',
    ticker         varchar(20)                        not null comment '자산 ticker',
    isin           varchar(20)                        not null comment '국제표준코드',
    asset_name     varchar(100)                       not null comment '자산 이름',
    init_listed_dt date                               not null comment '최초 상장일 (최초 등록일)',
    listed_dt      date                               not null comment '상장일 (등록일)',
    changed_dt     date                               not null comment '데이터 변경일',
    end_dt         date                               null comment '데이터 마지막으로 유효한 날짜 (changed_dt ~ end_dt 이 데이터가 유효한 기간임. null 이면 현재까지 유효한 데이터)',
    delisted_dt    date                               null comment '상장폐지일',
    changed_reason varchar(500)                       null comment '데이터 변경 사유',
    face_value     float                              null comment '액면가',
    market         varchar(20)                        null comment '시장 (KOSPI, KOSDAQ ...)',
    currency       varchar(10)                        null comment '통화 (KRW, USD ...)',
    min_order      float    default '1'               null comment '최저 매수 주문금액 (통화)',
    trading_unit   float    default '1'               null comment '최소 매매 단위 (주)',
    created_at     datetime default CURRENT_TIMESTAMP null,
    updated_at     datetime default CURRENT_TIMESTAMP null,
    constraint Operation_code_changed_dt_index
        unique (dn_id, changed_dt)
);

create index Operation_asset_code_index on operation (dn_id);
create index Operation_ticker_index on operation (ticker);
alter table operation
    add primary key (dn_id, changed_dt);

