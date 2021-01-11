import pandas as pd


def portfolio_diff(p1, p2):
    """
    포트폴리오 비교 p1 -> p2
    :param p1: series (index: ticker)
    :param p2: series (index: ticker)
    :return:
        result_sr (index: ticker, data: 양수일경우 buy, 음수일경우 sell)
    """
    result_sr = p2.sub(p1, fill_value=0)
    result_sr = result_sr[result_sr != 0]
    return result_sr


if __name__ == '__main__':
    _p1 = {'A006250': 19, 'A020000': 39, 'A006400': 4, 'A009720': 6, 'A016580': 190}
    _p1 = pd.Series(_p1)
    _p2 = {'A006251': 19, 'A020000': 35, 'A006400': 10, 'A009720': 6, 'A016580': 100, 'A016581': 90}
    _p2 = pd.Series(_p2)

    print(portfolio_diff(_p1, _p2))