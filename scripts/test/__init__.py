# 워런버핏 -- 260일 (일일)주가 변동성 하위 50%, PBR 하위 50%, ROE 상위 50%
# def buffet(universe_set, price_df, pbr_df, roe_df, vol_df, index_date):
#     # 투자대상 universe 만 추리기
#     price_df = price_df[universe_set]  # 투자대상 종목 제한 #1 코스피200 종목한정
#     pbr_df = pbr_df[universe_set]  # 투자대상 종목 제한 #1 코스피200 종목한정
#     roe_df = roe_df[universe_set]  # 투자대상 종목 제한 #1 코스피200 종목한정
#     vol_df = vol_df[universe_set]  # 투자대상 종목 제한 #1 코스피200 종목한정
#
#     vol = low_sorting(price_df, vol_df, index_date, None)  # 해당일자의 저vol주 소팅
#     vol['vol순위'] = vol.rank()
#
#     pbr = low_sorting(price_df, pbr_df, index_date, None)  # 해당일자의 저pbr주 소팅
#     pbr['pbr순위'] = pbr.rank()
#
#     roe = low_sorting(price_df, roe_df, index_date, None)  # 해당일자의 고roe주 소팅
#     roe['roe순위'] = roe.rank(ascending=False)
#
#     formula = pd.merge(vol, pbr, roe, how='outer', left_index=True, right_index=True)
#     formula['버핏공식 순위'] = (vol['vol순위'] + pbr['pbr순위'] + roe['roe순위']).rank().sort_values()
#     formula = formula.sort_values(by='버핏공식 순위')
#     return formula


if __name__ == '__main__':
    import pandas as pd
    from pandas_datareader import data

    start_date = '2019-04-17'
    end_date = '2020-04-17'

    def get_adj_close_df(ticker, s, e):
        data_df = data.DataReader(ticker, 'yahoo', s, e)
        data_df = data_df.resample('M').apply(lambda l: l[-1])
        return data_df['Adj Close'].rename(ticker).to_frame()


    aapl_df = get_adj_close_df('AAPL', start_date, end_date)
    goog_df = get_adj_close_df('GOOG', start_date, end_date)
    msft_df = get_adj_close_df('MSFT', start_date, end_date)

    # ERROR CASE: {TypeError}merge() got multiple values for argument 'how'
    # result_df = pd.merge(aapl_df, goog_df, msft_df, left_index=True, right_index=True, how='outer')

    # 가능한 방법
    # result_df = pd.merge(aapl_df, goog_df, left_index=True, right_index=True, how='outer')
    # result_df = pd.merge(result_df, msft_df, left_index=True, right_index=True, how='outer')

    # 추천하는 방법
    result_df = pd.concat([aapl_df, goog_df, msft_df], axis=1)