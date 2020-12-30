import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib
import six


def render_mpl_table(ax, df, font_size=14,
                     header_color='#40466e', row_colors=('#f1f1f2', 'w'), edge_color='w',
                     bbox=(0, 0, 1, 1),
                     draw_index=False,
                     font_family='NanumBarunGothic',
                     **kwargs):
    """
    input ax (axes) 객체에 Table 을 그린다.
    :param ax: axes 객체
    :param df: 그려질 table 객체
    :param font_size: 폰트크기
    :param header_color: header, index color
    :param row_colors: row 배경색. 객체에 들어가있는 color 가 순차적으로 그려진다.
    :param edge_color: 선색
    :param bbox: 그려질 범위 (left, top, right, bottom)
    :param draw_index: index 를 출력할지 여부
    :param font_family: 출력하고싶은 폰트
    :param kwargs: 기타 ax.table 함수에 들어갈 keyword arguments
    :return:
    """

    # matplotlib 에서 font 정보를 제대로 가지고 있지 않는경우가 있음
    matplotlib.font_manager._rebuild()

    # 마이너스글자 깨지는 경우 대비
    matplotlib.rcParams['axes.unicode_minus'] = False

    plt.rc('font', family=font_family)

    ax.axis('off')
    ax.grid(False)

    # index 를 그린다면 index 를 column 으로 취급하며 render 시에만 구분해준다.
    if draw_index:
        df = df.reset_index().copy()
        df.columns = ['', *df.columns[1:]]

    # table render
    mpl_table = ax.table(cellText=df.values, bbox=bbox, colLabels=df.columns, **kwargs)

    # 폰트 사이즈 변경
    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    # 세부적으로 cell 정보를 바꿔준다
    for k, cell in six.iteritems(mpl_table._cells):
        # 선 색깔 지정
        cell.set_edgecolor(edge_color)

        # column render
        if k[0] == 0:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        # index render
        elif draw_index and k[1] == 0:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
            cell._loc = 'right'
        # data render
        else:
            cell.set_facecolor(row_colors[k[0] % len(row_colors)])

    return ax


def draw_chart_and_table(return_df, perf_df):
    """
    수익률 chart 와 성과지표 table 을 그린다.
    :param return_df: 수익률 정보
    :param perf_df: 성과지표 정보
    :return:
    """
    # canvas spec
    plt.figure(figsize=(6, 6), dpi=128)

    # performance 를 아래쪽에 그리기위해 row 를 2로 설정한다.
    gs = matplotlib.gridspec.GridSpec(nrows=2,
                                      ncols=1,
                                      height_ratios=[2, 1],  # 위아래 canvas 비율
                                      width_ratios=[1])

    ax0 = plt.subplot(gs[0])
    ax1 = plt.subplot(gs[1])

    return_df.plot(ax=ax0)

    # draw legend. 오른쪽위 고정
    leg = ax0.legend(borderaxespad=1, loc='upper right')

    # draw table. 아래 고정
    render_mpl_table(ax1, perf_df.round(6), font_size=6.5,
                     bbox=[-0.05, -0.1, 1.05, 1.2],
                     cellLoc='left',
                     draw_index=True)

    plt.show()


if __name__ == '__main__':
    from pandas_datareader import data

    perf_df = pd.read_csv('sample.csv', index_col=0)

    start_date = '2019-04-17'
    end_date = '2020-04-17'
    aapl_data = data.DataReader('AAPL', 'yahoo', start_date, end_date)
    aapl_data = aapl_data.resample('M').apply(lambda l: l[-1])

    msft_data = data.DataReader('MSFT', 'yahoo', start_date, end_date)
    msft_data = msft_data.resample('M').apply(lambda l: l[-1])

    data_df = pd.concat([aapl_data['Adj Close'].rename('AAPL'), msft_data['Adj Close'].rename('MSFT')], axis=1)
    data_df = data_df.pct_change()

    draw_chart_and_table(data_df, perf_df)
