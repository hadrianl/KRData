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
# try:
#     import matplotlib.pyplot as plt
#     import matplotlib.finance as mpf
#     from matplotlib import ticker
#     import matplotlib.dates as mdates
# except ImportError as e:
#     Warning(f'导入matplotlib异常，检查matplotlib是否安装->{e}')

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


def draw_klines(df: pd.DataFrame, extra_lines=None, to_file=None):
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

    fig, [ax1, ax2] = plt.subplots(2, 1, sharex=True)
    fig.set_figheight(300 / 72)
    fig.set_figwidth(1200 / 72)
    ax1.set_position([0.1, 0.3, 0.8, 0.6])
    ax2.set_position([0.1, 0.1, 0.8, 0.15])

    ax1.set_title('KLine', fontsize='large', fontweight='bold')
    ax2.set_title('Volume')
    mpf.candlestick2_ochl(ax1, data_mat[1], data_mat[2], data_mat[3], data_mat[4], colordown='#53c156',
                          colorup='#ff1717', width=0.3, alpha=1)
    mpf.volume_overlay(ax2, data_mat[1], data_mat[2], data_mat[5], colordown='#53c156', colorup='#ff1717', width=0.3,
                       alpha=1)
    ax1.grid(True)
    ax2.grid(True)
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
    ax1.xaxis.set_major_locator(mdates.HourLocator())
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45],
                                                     interval=1))
    ax1.xaxis.set_major_locator(ticker.MaxNLocator(8))

    if extra_lines:
        for l in extra_lines:
            ax1.add_line(l)

    if 'trades' in df.columns:
        trades_long_x = []
        trades_long_y = []
        trades_long_s = []
        trades_short_x = []
        trades_short_y = []
        trades_short_s = []

        for i, (_, tl) in enumerate(df.trades.iteritems()):
            if isinstance(tl, Iterable):
                for t in tl:
                    if t['direction'] == 'long':
                        trades_long_x.append(i)
                        trades_long_y.append(t['price'])
                        trades_long_s.append(t['size'] * 100)
                    elif t['direction'] == 'short':
                        trades_short_x.append(i)
                        trades_short_y.append(t['price'])
                        trades_short_s.append(t['size'] * 100)
        else:
            ax1.scatter(trades_long_x, trades_long_y, s=trades_long_s, c='b', marker='^', alpha=0.7)
            ax1.scatter(trades_short_x, trades_short_y, s=trades_short_s, c='y', marker='v', alpha=0.8)

    if to_file:
        try:
            fig.savefig(to_file)
        except Exception as e:
            print('errSaveFig:', e)

    return fig


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


CODE_SUFFIX = ['1701', '1702', '1703', '1704', '1705', '1706', '1707', '1708', '1709', '1710', '1711', '1712',
               '1801', '1802', '1803', '1804', '1805', '1806', '1807', '1808', '1809', '1810', '1811', '1812']