create table operation
(
	ticker varchar(20) not null,
	isin varchar(20) not null,
	company_name varchar(100) not null,
	init_listed_dt date not null,
	listed_dt date not null,
	changed_dt date not null,
	end_dt date,
	delisted_dt date,
	changed_reason varchar(500),
	face_value double precision,
	market varchar(20),
	currency varchar(10),
	min_order double precision default '1'::double precision,
	trading_unit double precision default '1'::double precision,
	created_at timestamp default CURRENT_TIMESTAMP,
	updated_at timestamp default CURRENT_TIMESTAMP,
	constraint operation_pk
		primary key (ticker, changed_dt)
)
;

alter table operation owner to "stock-richgo"
;

create unique index operation_ticker_changed_dt_uindex
	on operation (ticker, changed_dt)
;

