import pandas as pd


def get_listed(file, date):
    """
    요청한 날짜의 종목 리스트를 가져온다.
    :param file: xlsx file
    :param date: 요청일
    :return:
    """
    df = pd.read_excel(file, header=5)
    df['Date'] = pd.to_datetime(df['Date'])
    return df[df['Date'] == date][['Code', 'Name']].set_index('Code')['Name']


def pivot_data(file):
    df = pd.read_excel(file, header=5)
    df['id'] = df.groupby('Date').cumcount()
    return df.pivot(index='Date', columns='id', values='Code')


if __name__ == '__main__':
    # print(get_listed('./resources/코스피200종목리스트-월별.xlsx', '20000131'))
    # pivot_data('./resources/코스피200종목리스트-월별.xlsx')
    test_dict = {}

    for i, x in enumerate(pd.date_range('20100101', '20150101', freq='M')):
        test_dict[x] = i

    print(test_dict)
