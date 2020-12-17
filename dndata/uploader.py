import pandas as pd

df = pd.read_excel('./resource/company.xlsx', 0, header=5)
kkk = 0

# fnguide_company_info
# Symbol - stock_code - VARCHAR(10)
# Name - codename - VARCHAR(30)
# 국제표준코드 - isin - CHAR(12) - NEW
# 종목명 (영문) - codename_en - VARCHAR(100) - NEW
# 설립일 - est_dt - DATE
# HomePage URL - hm_url - VARCHAR(100)
# 상장(등록)일자 (listing date) - lt_dt - DATE - NEW
# 상장폐지일자 (delisting date) - dlt_dt - DATE - NEW
# 시장이전일 (transfer of listing date) - tlt_dt - DATE - NEW
# 시장이전내용 (description of listing transfer) - tlt_desc - VARCHAR(500) - NEW
# 거래소 업종구분 (exchange category) - ex_ind - VARCHAR(45)
# 한국표준산업분류10차(세세분류) - kr_ind_5 - VARCHAR(100)
# 한국표준산업분류코드10차(세세분류) - kr_ind_5_cd - int
# 한국표준산업분류10차(세분류) - kr_ind_4 - VARCHAR(100)
# 한국표준산업분류코드10차(세분류) - kr_ind_4_cd - int
# 한국표준산업분류10차(소분류) - kr_ind_3 - VARCHAR(100)
# 한국표준산업분류코드10차(소분류) - kr_ind_3_cd - int
# 한국표준산업분류10차(중분류) - kr_ind_2 - VARCHAR(100)
# 한국표준산업분류코드10차(중분류) - kr_ind_2_cd - int
# 한국표준산업분류10차(대분류) - kr_ind_1 - VARCHAR(100)
# 한국표준산업분류코드10차(대분류) - kr_ind_1_cd - CHAR(1)
# MKF500 편입여부 - mkf500_yn - CHAR(1)
# created_at - DATETIME
# updated_at - DATETIME


# fnguide_trading
# 기준일 - std_dt - DATE - NEW
# stock_code - VARCHAR(10)
# adjusted_price - float
# vol_52w - float
# created_at - DATETIME
# updated_at - DATETIME


# fnguide_performance_daily
# 기준일 - std_dt - DATE
# Symbol - stock_code - VARCHAR(10)
# dividend_yield_ratio - float
# per - float
# pbr - float
# pcr - float
# psr - float
# created_at - DATETIME
# updated_at - DATETIME


# fnguide_performance_period
# 기준일 - std_dt - DATE
# Symbol - stock_code - VARCHAR(10)
# freq - CHAR(1)
# roa - float
# roe - float
# created_at - DATETIME
# updated_at - DATETIME



# Symbol - symbol - CHAR(7)
# Name - name - VARCHAR(100)
# 국제표준코드 - isin
# Ticker - X
# 종목명 (Full) - X
# 종목명 (영문) - name_en - VARCHAR(100)
# 종목명 (영문,약) - X
# 기업명 (한글) - X
# 기업명 (영문) - X
# 사업자등록번호 - X
# 법인등록번호 - X
# 설립일 - establishment_date - DATE
# 본사우편번호 - X
# 본사주소 - X
# 본사전화번호 - X
# 서울사무소우편번호 - X
# 서울사무소주소 - X
# 서울사무소전화번호 - X
# IR담당자 전화번호 - X
# 대리인(명의개서) - X
# 주거래은행 - X
# 주거래은행-지점 - X
# 공고신문 - X
# HomePage URL - url - VARCHAR(100)
# 상장(등록)일자 - DATE
# 상장폐지일자 - DATE
# 시장이전일 - DATE
# 시장이전내용 - VARCHAR(500)
# 거래소 업종구분 - VARCHAR(50)
# 한국표준산업분류10차(세세분류) - VARCHAR(100)
# 한국표준산업분류코드10차(세세분류) - int
# 한국표준산업분류10차(세분류) - VARCHAR(100)
# 한국표준산업분류코드10차(세분류) - int
# 한국표준산업분류10차(소분류) - VARCHAR(100)
# 한국표준산업분류코드10차(소분류) - int
# 한국표준산업분류10차(중분류) - VARCHAR(100)
# 한국표준산업분류코드10차(중분류) - int
# 한국표준산업분류10차(대분류) - VARCHAR(100)
# 한국표준산업분류코드10차(대분류) - CHAR(1)
# MKF500 편입여부 - MKF500 - CHAR(1)
# K-IFRS적용년월 - X
# 통화(ISO) - X
# 지역 - X
# 국가 - X
