#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/21 0021 12:49
# @Author  : Hadrianl 
# @File    : HKData


import pandas as pd
from dateutil import parser
import datetime as dt
from . import BaseData
from .util import _check_ktype, CODE_SUFFIX
import pymongo as pmg
from collections import OrderedDict


class HKFuture(BaseData):
    def __init__(self, user=None, pwd=None, host='192.168.2.226', port=27017, db='HKFuture'):
        super(HKFuture, self).__init__(host, port, db, user=user, pwd=pwd )

    def get_bars(self, code:str, fields:list=None, start:(str, dt.datetime)=None, end:(str, dt.datetime)=None, bar_counts:int=None, ktype:str='1min') -> pd.DataFrame:
        '''
        获取k线数据，按天提取
        :param code: 代码
        :param start: None为从最开始查询
        :param end: None为查询到最新的bar
        :param ktype: [1, 5, 15, 30, 60]分钟m或者min,或者1D
        :return:
        '''
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        start = start if start is not None else dt.datetime(1970, 1, 1)
        end = end if end is not None else dt.datetime(2050, 1, 1)

        code = code.upper()

        if bar_counts is not None and end is not None:
            ret = self.__get_bars_by_count(code, end, bar_counts, ktype, fields)  # 根据查询数量来查询， 性能更优
        else:
            ret = self.__get_bars_by_daterange(code, start, end, ktype, fields)  # 根据日期范围来查询

        return ret


    def get_available_contracts(self, underlying:str, date: (str, dt.datetime)):
        if isinstance(date, str):
            _date = parser.parse(date).replace(hour=0, minute=0, second=0)
        elif isinstance(date, dt.datetime):
            _date = date.replace(hour=0, minute=0, second=0)
        else:
            raise ValueError('请输入str或者datetime类型')

        underlying = underlying.upper()

        contract_info = pd.DataFrame([ci for ci in self._db.get_collection('future_contract_info').find({'CLASS_CODE': underlying, 'DATE': _date})])

        return contract_info

    def get_trading_dates(self, start:(str, dt.datetime), end:(str, dt.datetime), code:str=None, underlying:str=None):
        """
        填写code或者underlying参数，优先使用code
        :param start:
        :param end:
        :param code:
        :param underlying:
        :return:
        """
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        start = start.replace(hour=0, minute=0, second=0) if start is not None else dt.datetime(1970, 1, 1)
        end = end.replace(hour=0, minute=0, second=0) if end is not None else dt.datetime(2050, 1, 1)

        if isinstance(code, str):
            trade_date = list(
                set(td['DATE'] for td in self._db.get_collection('future_contract_info').find({'CODE': code,
                                                                                               'DATE': {'$gte': start,
                                                                                                        '$lte': end}},
                                                                                              ['DATE'])))
            trade_date.sort()
            return trade_date
        elif isinstance(underlying, str):
            trade_date = list(
                set(td['DATE'] for td in self._db.get_collection('future_contract_info').find({'CLASS_CODE': underlying,
                                                                                               'DATE': {'$gte': start,
                                                                                                        '$lte': end}},
                                                                                              ['DATE'])))
            trade_date.sort()
            return trade_date
        else:
            raise Exception('请输入正确的code或者underlying')

    def get_main_contract_bars(self, underlying:str, fields:list=None, start:(str, dt.datetime)=None, end:(str, dt.datetime)=None, ktype:str='1min'):
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        underlying = underlying.upper()
        trade_date = self.get_trading_dates(start, end, underlying=underlying)

        data = []
        code_daterage = OrderedDict()
        _s = start
        for i, td in enumerate(trade_date):
            _c_info = self.get_available_contracts(underlying, td)

            if _c_info.loc[0, 'EXPIRY_DATE'] not in [trade_date[i:i+2]]:
                code = _c_info.loc[0, 'CODE']
            else:
                code = _c_info.loc[1, 'CODE']

            dr = code_daterage.setdefault(code, [])
            dr.append(td)

        for c, d in code_daterage.items():
            data.append(self.__get_bars_by_daterange(c, d[0], d[-1], ktype, fields))

        df = pd.concat(data)
        return df

    @staticmethod
    def draw_klines(df:pd.DataFrame):
        import matplotlib.pyplot as plt
        import matplotlib.finance as mpf
        from matplotlib import ticker
        import matplotlib.dates as mdates
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


        fig, ax1,  = plt.subplots(figsize=(1200 / 72, 480 / 72))
        plt.title('KLine', fontsize='large',fontweight = 'bold')
        mpf.candlestick2_ochl(ax1, data_mat[1], data_mat[2], data_mat[3], data_mat[4], colordown='#53c156', colorup='#ff1717', width=0.3, alpha=1)
        ax1.grid(True)
        ax1.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
        ax1.xaxis.set_major_locator(mdates.HourLocator())
        ax1.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 15, 30, 45],
                                                        interval=1))
        ax1.xaxis.set_major_locator(ticker.MaxNLocator(8))

    def __get_bars_by_count(self, code, current_dt, bar_counts, ktype, fields):
        col = self._db.get_collection(f'future_{ktype}_')
        data = [v for v in col.find({'code':code, 'datetime': {'$lte': current_dt}}, limit=bar_counts, sort=[(('datetime', pmg.DESCENDING))])]
        data.reverse()
        data = pd.DataFrame(data).set_index('datetime', drop=False)
        if fields is None:
            fields = ['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'trade_date']

        data = data.loc[:, fields]
        return data

    def __get_bars_by_daterange(self, code, start, end, ktype, fields):
        _fields = ['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'trade_date']
        col = self._db.get_collection(f'future_{ktype}_')

        data = [ret for ret in col.find(
            {'code': code, 'trade_date': {'$gte': start,
                                        '$lte': end}}, _fields,
            sort=[('datetime', pmg.ASCENDING)])]

        df = pd.DataFrame(data, columns=['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'trade_date'])
        df.set_index('datetime', drop=False, inplace=True)

        if fields is None:
            fields = [field for field in df.columns]
        else:
            fields = [field for field in fields if field in df.columns]

        return df.loc[:, fields]

    def _resample_raw(self,  code, fields=None, start=None, end=None, ktype='1min'):
        def __get_bars_by_daterange(code, start, end, ktype, fields):
            trade_date = self.get_trading_dates(start, end, code=code)

            ktype = _check_ktype(ktype)
            data = []
            for td in trade_date:
                data.extend(__get_whole_date_trade(code, td))

            if data == []:
                df = pd.DataFrame()
            else:
                df = __format_data(data, fields, ktype)

            return df

        def __format_data(data, fields, ktype):  # 格式整理
            df = pd.DataFrame(data, columns=['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'date_stamp',
                                             'trade_date'])
            df.set_index('datetime', drop=False, inplace=True)
            apply_func_dict = {'datetime': 'last',
                               'code': 'first',
                               'open': 'first',
                               'high': 'max',
                               'low': 'min',
                               'close': 'last',
                               'volume': 'sum',
                               'trade_date': 'first'
                               }
            if ktype != '1D':
                resampled_df = df.resample(ktype).apply(apply_func_dict)
            else:
                resampled_df = df.resample(ktype, on='trade_date').apply(apply_func_dict)
            resampled_df.dropna(how='all', inplace=True)
            if fields is None:
                fields = [field for field in resampled_df.columns]
            else:
                fields = [field for field in fields if field in resampled_df.columns]

            return resampled_df.loc[:, fields]

        def __get_whole_date_trade(code, trade_date):  # 获取某一天夜盘+早盘的全部数据
            _fields = ['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'date_stamp']
            td = trade_date
            d = [ret for ret in self._col.find(
                {'code': code, 'datetime': {'$gte': td.replace(hour=9, minute=14, second=0),
                                            '$lt': td.replace(hour=17, minute=0, second=0)}}, _fields,
                sort=[('datetime', pmg.DESCENDING)])]

            _bar_before_d = self._col.find_one(
                {'code': code, 'datetime': {'$lt': td.replace(hour=9, minute=14, second=0)}},
                _fields,
                sort=[('datetime', pmg.DESCENDING)])

            if _bar_before_d is not None:
                if dt.time(17, 14) < _bar_before_d['datetime'].time() <= dt.time(23, 59):
                    _d_aht = self._col.find({'code': code, 'type': '1min',
                                             'datetime': {'$gte': dt.datetime.fromtimestamp(
                                                 _bar_before_d['date_stamp']) + dt.timedelta(hours=17,
                                                                                             minutes=14),
                                                          '$lte': dt.datetime.fromtimestamp(
                                                              _bar_before_d['date_stamp']) + dt.timedelta(
                                                              hours=23, minutes=59)}},
                                            _fields,
                                            sort=[('datetime', pmg.DESCENDING)])
                    d_aht = [i for i in _d_aht]
                    d = d + d_aht
                elif dt.time(0, 0) < _bar_before_d['datetime'].time() <= dt.time(2, 0):
                    _d_aht = self._col.find({'code': code, 'type': '1min',
                                             'datetime': {'$gte': dt.datetime.fromtimestamp(
                                                 _bar_before_d['date_stamp']) - dt.timedelta(hours=6,
                                                                                             minutes=46),
                                                          '$lte': dt.datetime.fromtimestamp(
                                                              _bar_before_d['date_stamp']) + dt.timedelta(
                                                              hours=2, minutes=0)}},
                                            _fields,
                                            sort=[('datetime', pmg.DESCENDING)])
                    d_aht = [i for i in _d_aht]
                    d = d + d_aht
            d.reverse()
            for _d in d:
                _d['trade_date'] = td

            return d


        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        code = code.upper()

        ret = __get_bars_by_daterange(code, start, end, ktype, fields)  # 根据日期范围来查询

        return ret