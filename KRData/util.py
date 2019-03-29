#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/21 0021 12:57
# @Author  : Hadrianl 
# @File    : util

import re
import pandas as pd
try:
    import matplotlib.pyplot as plt
    import matplotlib.finance as mpf
    from matplotlib import ticker
    import matplotlib.dates as mdates
except ImportError as e:
    Warning(f'导入matplotlib异常，检查matplotlib是否安装->{e}')

def _check_ktype(ktype):
    _ktype = re.findall(r'^(\d+)([a-zA-Z]+)$', ktype)[0]
    if _ktype:
        _n = int(_ktype[0])
        _t = _ktype[1].lower()
        if _t in ['m', 'min']:
            _t = 'T'
            if _n not in [1, 5, 15, 30, 60]:
                raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')
        elif _t in ['d', 'day']:
            _t = 'D'
            if _n not in [1]:
                raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')
        else:
            raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')
    else:
        raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')

    return f'{_n}{_t}'


def draw_klines(df:pd.DataFrame, extra_lines=None, to_file=None):
    columns = ['datetime', 'open', 'close', 'high', 'low', 'volume']
    if not set(df.columns).issuperset(columns):
        raise Exception(f'请包含{columns}字段')

    data = df.loc[:, columns]

    data_mat = data.as_matrix().T

    xdate = data['datetime'].tolist()

    def mydate(x, pos):
        try:
            return xdate[int(x)]
        except IndexError:
            return ''
    fig=plt.gcf()
    fig.set_figheight(480 / 72)
    fig.set_figwidth(1200 / 72)
    ax1=fig.gca()
    ax1.cla()
    # fig, ax1,  = plt.subplots(figsize=(1200 / 72, 480 / 72))
    plt.title('KLine', fontsize='large',fontweight = 'bold')
    mpf.candlestick2_ochl(ax1, data_mat[1], data_mat[2], data_mat[3], data_mat[4], colordown='#53c156', colorup='#ff1717', width=0.3, alpha=1)
    ax1.grid(True)
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
    ax1.xaxis.set_major_locator(mdates.HourLocator())
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45],
                                                    interval=1))
    ax1.xaxis.set_major_locator(ticker.MaxNLocator(8))


    if extra_lines:
        for l in extra_lines:
            ax1.add_line(l)

    if to_file:
        try:
            fig.savefig(to_file)
        except Exception as e:
            print('errSaveFig:', e)

    return fig


CODE_SUFFIX = ['1701', '1702', '1703', '1704', '1705', '1706', '1707', '1708', '1709', '1710', '1711', '1712',
               '1801', '1802', '1803', '1804', '1805', '1806', '1807', '1808', '1809', '1810', '1811', '1812']