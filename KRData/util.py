#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/21 0021 12:57
# @Author  : Hadrianl 
# @File    : util

import re
import pandas as pd
from logging.handlers import SMTPHandler
import logging
import time
import sys
from typing import Iterable
from pathlib import Path
import json
import datetime as dt
import numpy as np
# try:
#     import matplotlib.pyplot as plt
#     import matplotlib.finance as mpf
#     from matplotlib import ticker
#     import matplotlib.dates as mdates
# except ImportError as e:
#     Warning(f'导入matplotlib异常，检查matplotlib是否安装->{e}')

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]



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


def draw_klines(df: pd.DataFrame, main_chart_lines=None, sub_chart_lines=None, *, annotate=False, to_file=None):
    """
    基础画图方法
    :param df: dataframe->基础需包含['datetime', 'open', 'close', 'high', 'low', 'volume']，
                可选trades: List[Dict]，若有则会添加交易标识Dict中包含字段['price', 'size', 'direction']
    :param extra_lines: 添加想要的线
    :param to_file:  生成图片，默认None为不生成
    :return:
    """
    import matplotlib.pyplot as plt
    import mpl_finance as mpf
    from matplotlib import ticker
    import matplotlib.dates as mdates

    columns = ['datetime', 'open', 'close', 'high', 'low', 'volume']
    if not set(df.columns).issuperset(columns):
        raise Exception(f'请包含{columns}字段')

    data = df.loc[:, columns]

    data_mat = data.values.T

    xdate = data['datetime'].tolist()

    def mydate(x, pos):
        try:
            return xdate[int(x)]
        except IndexError:
            return ''

    data_len = len(data_mat[2])

    fig, [ax1, ax2, ax3] = plt.subplots(3, 1, sharex=True)
    fig.set_figheight(300 / 72)
    fig.set_figwidth(1200 / 72)
    ax1.set_position([0.1, 0.4, 0.8, 0.55])
    ax2.set_position([0.1, 0.2, 0.8, 0.15])
    ax3.set_position([0.1, 0.05, 0.8, 0.1])

    ax1.set_title('KLine', fontsize='large', fontweight='bold')
    ax2.set_title('MACD')
    ax3.set_title('Volume')
    mpf.candlestick2_ochl(ax1, data_mat[1], data_mat[2], data_mat[3], data_mat[4], colordown='#53c156',
                          colorup='#ff1717', width=min(120 / data_len, 1), alpha=1)
    mpf.volume_overlay(ax3, data_mat[1], data_mat[2], data_mat[5], colordown='#53c156', colorup='#ff1717', width=min(120 / data_len, 1),
                       alpha=1)
    ax1.grid(True)
    ax2.grid(True)
    ax3.grid(True)
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
    ax1.xaxis.set_major_locator(mdates.HourLocator())
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45],
                                                     interval=1))
    ax1.xaxis.set_major_locator(ticker.MaxNLocator(8))

    if main_chart_lines:
        for l in main_chart_lines:
            ax1.add_line(l)
    else:
        import talib
        l = range(data_len)
        for ma, c in zip([5, 10, 30, 60], ['r', 'b', 'g', 'y']):
            ma_line = mpf.Line2D(l, talib.MA(data_mat[2].astype(float), ma), color=c, linewidth=120 / data_len)
            ax1.add_line(ma_line)

    if sub_chart_lines:
        for l in main_chart_lines:
            ax2.add_line(l)
    else:
        import talib
        dif, dea, macd = talib.MACDEXT(data_mat[2].astype(float), fastperiod=12, fastmatype=1, slowperiod=26, slowmatype=1, signalperiod=9, signalmatype=1)
        macd = macd * 2
        l = range(data_len)
        dif_line = mpf.Line2D(l, dif, color='b', linewidth=120 / data_len)
        dea_line = mpf.Line2D(l, dea, color='y', linewidth=120 / data_len)
        ax2.add_line(dif_line)
        ax2.add_line(dea_line)
        mpf.candlestick2_ochl(ax2, [0]*len(macd), macd,
                              np.where(macd >= 0, macd, 0), np.where(macd < 0, macd, 0), colordown='#53c156',
                              colorup='#ff1717', width=120 / data_len, alpha=0.7)

    if 'trades' in df.columns:
        trades_long_x = []
        trades_long_y = []
        trades_long_s = []
        trades_short_x = []
        trades_short_y = []
        trades_short_s = []

        size_ratio = 100 * 120 / data_len
        for i, (_, tl) in enumerate(df.trades.iteritems()):
            if isinstance(tl, Iterable):
                long_n = 0
                short_n = 0
                total_long = 0
                total_short = 0
                for t in tl:
                    if t['direction'] == 'long':
                        trades_long_x.append(i)
                        trades_long_y.append(t['price'])
                        trades_long_s.append(t['size'] * size_ratio)
                        long_n += t['size']
                        total_long += t['price']
                    elif t['direction'] == 'short':
                        trades_short_x.append(i)
                        trades_short_y.append(t['price'])
                        trades_short_s.append(t['size'] * size_ratio)
                        short_n += t['size']
                        total_short += t['price']
                else:
                    if annotate and long_n:
                        avg_long_price = total_long/long_n
                        ax1.annotate(f'+{long_n}@{avg_long_price:.1f}', xy=(i, avg_long_price),
                                     xytext=(i+0.1, avg_long_price - 0.1))
                    if annotate and short_n:
                        avg_short_price = total_short / short_n
                        ax1.annotate(f'-{short_n}@{avg_short_price:.1f}', xy=(i, avg_short_price),
                                     xytext=(i + 0.1, avg_short_price + 0.1))
        else:
            ax1.scatter(trades_long_x, trades_long_y, s=trades_long_s, c='b', marker='^', alpha=1, linewidths=0, zorder=2)
            ax1.scatter(trades_short_x, trades_short_y, s=trades_short_s, c='y', marker='v', alpha=1, linewidths=0, zorder=2)

    if to_file:
        try:
            fig.savefig(to_file, dpi=200)
        except Exception as e:
            print('errSaveFig:', e)

    return fig


def trade_review_in_notebook(trades=None):
    """
    用于查看某账户的具体日期的成交明细
    :return:
    """
    from .IBData import IBTrade
    import ipywidgets as widgets
    from IPython.display import display

    ibt = IBTrade()

    account = widgets.Text(continuous_update=False, description='account')
    symbol = widgets.Text(continuous_update=False, description='symbol')
    source = widgets.RadioButtons(options=['IB', 'HK'], description='source')
    range_ = widgets.IntRangeSlider(continuous_update=False, description='range')
    expand_offset = widgets.IntSlider(min=0, max=500, step=10, value=60, continuous_update=False,
                                      description='expand_offset')

    start_date = widgets.DatePicker()
    end_date = widgets.DatePicker()

    time_box = widgets.HBox([start_date, end_date])

    def update_time_range(*args):
        sd = start_date.value
        ed = end_date.value
        if sd and ed:
            start = dt.datetime(sd.year, sd.month, sd.day)
            end = dt.datetime(ed.year, ed.month, ed.day)
            minute_count = (end - start).total_seconds() // 60
            range_.set_trait('max', minute_count)
            range_.set_trait('value', (0, minute_count))
            print(range_.max)

    def update_time_range_str(*args):
        sd = start_date.value
        ed = end_date.value
        if sd and ed:
            start = dt.datetime(sd.year, sd.month, sd.day)
            _from.value = str(start + dt.timedelta(minutes=range_.value[0]))
            _to.value = str(start + dt.timedelta(minutes=range_.value[1]))

    _from = widgets.Text(value='', placeholder='yyyymmdd HH:MM:SS', description='From:', disabled=True)
    _to = widgets.Text(value='', placeholder='yyyymmdd HH:MM:SS', description='To:', disabled=True)
    time_range_box = widgets.HBox([_from, _to])

    start_date.observe(update_time_range, 'value')
    end_date.observe(update_time_range, 'value')
    start_date.observe(update_time_range_str, 'value')
    end_date.observe(update_time_range_str, 'value')
    range_.observe(update_time_range_str, 'value')

    def show_data(account, symbol, start_date, end_date, range_, expand_offset, source):
        print(account, symbol, start_date, end_date, range_, expand_offset, source)
        if account and symbol and start_date and end_date:
            start = dt.datetime(start_date.year, start_date.month, start_date.day)
            end = dt.datetime(end_date.year, end_date.month, end_date.day)
            ibt.account = account
            fills = ibt[start + dt.timedelta(minutes=range_[0]): start + dt.timedelta(minutes=range_[1]):symbol]
            ibt.display_trades(fills, expand_offset=expand_offset, mkdata_source=source)

    params = {'account': account, 'symbol': symbol,
              'start_date': start_date,
              'end_date': end_date,
              'range_': range_,
              'expand_offset': expand_offset,
              'source': source}
    out = widgets.interactive_output(show_data, params)
    display(account, symbol, source, time_box, range_, expand_offset, time_range_box, out)


def trade_review_in_notebook2(symbol=None, mkdata=None, executions: list = None, account=None, date_from=None,
                             date_to=None, expand_offset=120, source='IB'):
    import ipywidgets as widgets
    from IPython.display import display
    from dateutil import parser
    import datetime as dt
    import talib
    import pandas as pd
    import mpl_finance as mpf

    if isinstance(expand_offset, tuple):
        s_offset = expand_offset[0]
        e_offset = expand_offset[1]
    else:
        s_offset = e_offset = expand_offset

    if account and date_from and symbol:
        # 在只填写accout与date_from的情况下，找出date_from当天，account中的所有交易记录
        from KRData.IBData import IBTrade
        ibt = IBTrade(account)
        date_from = parser.parse(date_from) if isinstance(date_from, str) else date_from
        date_to = date_from + dt.timedelta(days=1) if date_to is None else (
            parser.parse(date_to) if isinstance(date_to, str) else date_to)
        fills = ibt[date_from:date_to:symbol]
        if fills.count() < 1:
            raise Exception(f'账户：{account}在{date_from}-{date_to}不存在交易记录')
        conId = fills[0].contract.conId
        executions = [{'datetime': f.time.replace(second=0) + dt.timedelta(hours=8), 'price': f.execution.price,
                       'size': f.execution.shares, 'direction': 'long' if f.execution.side == 'BOT' else 'short'} for f
                      in fills]
        executions.sort(key=lambda e: e['datetime'])
        start = executions[0]['datetime'] - dt.timedelta(minutes=s_offset)
        end = executions[-1]['datetime'] + dt.timedelta(minutes=e_offset)
        if source == 'IB':
            mkdata = ibt._ib_market.get_bars_from_ib(conId, start, end)
        elif source == 'HK':
            from KRData.HKData import HKFuture
            hf = HKFuture()
            mkdata = hf.get_bars(symbol, start=start, end=end, queryByDate=False)
            del hf
        del ibt
    elif mkdata is None and symbol:
        # 在只填写execution的情况下，自动根据source获取数据
        for e in executions:
            if isinstance(e['datetime'], str):
                e['datetime'] = parser.parse(e['datetime'])

            e['datetime'] = e['datetime'].replace(second=0)

        if executions:
            executions.sort(key=lambda e: e['datetime'])  # 交易执行排序

        start = executions[0]['datetime'] - dt.timedelta(minutes=s_offset)
        end = executions[-1]['datetime'] + dt.timedelta(minutes=e_offset)

        if source == 'IB':
            from KRData.IBData import IBMarket
            ibm = IBMarket()
            mkdata = ibm.get_bars_from_ib(symbol, start, end)
            del ibm
        elif source == 'HK':
            from KRData.HKData import HKFuture
            hf = HKFuture()
            mkdata = hf.get_bars(symbol, start=start, end=end, queryByDate=False)
            del hf

    mkdata['ma5'] = talib.MA(mkdata['close'].values, timeperiod=5)
    mkdata['ma10'] = talib.MA(mkdata['close'].values, timeperiod=10)
    mkdata['ma30'] = talib.MA(mkdata['close'].values, timeperiod=30)
    mkdata['ma60'] = talib.MA(mkdata['close'].values, timeperiod=60)

    executions_df = pd.DataFrame(executions).set_index('datetime')
    executions_df_grouped = executions_df.groupby('datetime').apply(lambda df: df.to_dict('records'))
    executions_df_grouped.name = 'trades'
    mkdata = mkdata.merge(executions_df_grouped, 'left', left_index=True, right_index=True)

    symbolWidget = widgets.Text(value=str(symbol), description='symbol', disable=True)

    fromWidget = widgets.Text(value='', placeholder='yyyymmdd HH:MM:SS', description='From:', disabled=True)
    toWidget = widgets.Text(value='', placeholder='yyyymmdd HH:MM:SS', description='To:', disabled=True)
    time_range_box = widgets.HBox([fromWidget, toWidget])

    offsetWidget = widgets.IntSlider(min=0, max=500, step=10, value=60, description='expand_offset')

    executionSelection = widgets.SelectionRangeSlider(
        options=[(str(_e['datetime']), _e['datetime']) for _e in executions],
        index=(0, len(executions) - 1),
        description='execution',
        disabled=False,
        continuous_update=False,
        )

    fig = None

    def save_fig(b):
        print(b)
        print(fig)
        if fig is not None:
            try:
                fig.savefig('fig.png')
            except Exception as e:
                print('errSaveFig:', e)

    saveFigButton = widgets.Button(description='保存图片')
    saveFigButton.on_click(save_fig)

    def show_data(e_select, offset):
        nonlocal fig
        s = e_select[0] - dt.timedelta(minutes=offset)
        e = e_select[-1] + dt.timedelta(minutes=offset)

        fromWidget.value = str(s)
        toWidget.value = str(e)

        temp_mkdata = mkdata[s:e]
        l = range(len(temp_mkdata))
        lines = []
        for ma, c in zip(['ma5', 'ma10', 'ma30', 'ma60'], ['r', 'b', 'g', 'y']):
            lines.append(mpf.Line2D(l, temp_mkdata[ma], color=c))

        fig = draw_klines(temp_mkdata, main_chart_lines=lines)

    params = {'e_select': executionSelection, 'offset': offsetWidget}

    out = widgets.interactive_output(show_data, params)
    display(symbolWidget, time_range_box, executionSelection, offsetWidget, saveFigButton, out)


def trade_review_in_notebook3(symbol, executions: list = None, pnlType='session'):
    import ipywidgets as widgets
    from IPython.display import display
    from .HKData import HKFuture

    if executions:
        executions.sort(key=lambda e: e['datetime'])  # 交易执行排序

    open_close_match = []
    if pnlType == 'session':
        from collections import deque
        pos_queue = deque()
        match = []
        for i, e in enumerate(executions):
            pos_queue.append((i, e['size']) if e['direction'] == 'long' else (i, -e['size']))
            match.append(i)
            if sum(p[1] for p in pos_queue) == 0:
                pos_queue.clear()
                open_close_match.append(match)
                match = []
        else:
            if match:
                open_close_match.append(match)

    matchSelection = widgets.SelectMultiple(
        options=[(str(m), m) for m in open_close_match],
        rows=20,
        layout=widgets.Layout(width='100%', height='100px'),
        description='match',
        disabled=False
    )
    symbolWidget = widgets.Text(value=str(symbol), description='symbol', disable=True)
    figSuffixWidget = widgets.Text(value='', description='figSuffix')
    offsetWidget = widgets.IntSlider(min=0, max=500, step=10, value=60, continuous_update=False,
                                     description='expand_offset')

    fig = None

    def save_fig(b):
        if fig is not None:
            try:
                fig.savefig(f'{symbol}_{figSuffixWidget.value}.png')
            except Exception as e:
                print('errSaveFig:', e)

    saveFigButton = widgets.Button(description='保存图片')
    saveFigButton.on_click(save_fig)

    savfig_box = widgets.HBox([saveFigButton, figSuffixWidget])

    hf = HKFuture()

    def show_data(match, expand_offset):
        nonlocal fig
        es = []
        for m in match:
            for i in m:
                es.append(executions[i])
        if not es:
            print('请选择execution')
            return
        fig = hf.display_trades(symbol, es, expand_offset=expand_offset)

    params = {'match': matchSelection, 'expand_offset': offsetWidget}

    out = widgets.interactive_output(show_data, params)
    display(symbolWidget, matchSelection, offsetWidget, savfig_box, out)



class SSLSMTPHandler(SMTPHandler):
    """
    支持SSL的SMTPHandler
    """

    def __init__(self, mailhost, fromaddr, toaddrs: tuple, subject,
                 credentials=None, secure=None, timeout=5.0,  mail_time_interval=0):
        super().__init__(mailhost, fromaddr, toaddrs, subject,
                         credentials, secure, timeout)
        self._time_interval = mail_time_interval
        self._msg_map = dict()  # 是一个内容为键时间为值得映射

    def emit(self, record: logging.LogRecord):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        from threading import Thread
        if sys.getsizeof(self._msg_map) > 10 * 1000 * 1000:
            self._msg_map.clear()
        Thread(target=self.__emit, args=(record,)).start()

    def __emit(self, record):
        if record.msg not in self._msg_map or time.time() - self._msg_map[record.msg] > self._time_interval:
            try:
                import smtplib
                from email.mime.multipart import MIMEMultipart
                from email.mime.text import MIMEText
                import email.utils
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_SSL_PORT
                smtp = smtplib.SMTP_SSL(self.mailhost, port, timeout=self.timeout)
                msgRoot = MIMEMultipart()

                msgRoot['From'] = self.fromaddr
                msgRoot['To'] = ','.join(self.toaddrs)
                msgRoot['Subject'] = self.getSubject(record)
                # msgRoot['Date'] = email.utils.localtime()
                part = MIMEText(self.format(record), _charset="utf-8")
                msgRoot.attach(part)

                IBlogFile = getattr(record, 'IBlogFile', None)
                if IBlogFile:
                    from email.mime.application import MIMEApplication
                    try:
                        logFile = MIMEApplication(open(IBlogFile, "rb").read())
                        logFile.add_header('Content-Disposition', 'attachment', filename=IBlogFile)
                        msgRoot.attach(logFile)
                    except Exception as e:
                        print(e)

                if self.username:
                    if self.secure is not None:
                        smtp.ehlo()
                        smtp.starttls(*self.secure)
                        smtp.ehlo()
                    smtp.login(self.username, self.password)
                smtp.send_message(msgRoot)
                smtp.quit()
                self._msg_map[record.msg] = time.time()
            except Exception:
                self.handleError(record)
        else:
            pass

def _get_KR_settings_dir(temp_name: str):
    """
    Get path where trader is running in.
    """
    cwd = Path.cwd()
    temp_path = cwd.joinpath(temp_name)

    # If .vntrader folder exists in current working directory,
    # then use it as trader running path.
    if temp_path.exists():
        return cwd, temp_path

    # Otherwise use home path of system.
    home_path = Path.home()
    temp_path = home_path.joinpath(temp_name)

    # Create .vntrader folder under home path if not exist.
    if not temp_path.exists():
        temp_path.mkdir()

    return home_path, temp_path

HOME_DIR, TEMP_DIR = _get_KR_settings_dir('.KRData')

def load_json_settings(filename: str):
    """
    Load data from json file in temp path.
    """
    filepath = TEMP_DIR.joinpath(filename)

    if filepath.exists():
        with open(filepath, mode='r') as f:
            data = json.load(f)
        return data
    else:
        save_json_settings(filename, {})
        return {}

def save_json_settings(filename: str, data: dict):
    """
    Save data into json file in temp path.
    """
    filepath = TEMP_DIR.joinpath(filename)
    with open(filepath, mode='w+') as f:
        json.dump(data, f, indent=4)



CODE_SUFFIX = ['1701', '1702', '1703', '1704', '1705', '1706', '1707', '1708', '1709', '1710', '1711', '1712',
               '1801', '1802', '1803', '1804', '1805', '1806', '1807', '1808', '1809', '1810', '1811', '1812']