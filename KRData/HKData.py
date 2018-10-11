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

class HKFuture(BaseData):
    def __init__(self, host='192.168.2.226', port=27017, db='HKFuture'):
        super(HKFuture, self).__init__(host, port, db)

    def get_bars(self, code, fields=None, start=None, end=None, ktype='1m'):
        '''
        获取k线数据，历史k线从数据库拿，当日从tdx服务器提取,按天提取数据
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

        end = end + dt.timedelta(days=1) if end is not None else end

        ktype = _check_ktype(ktype)
        _fields = ['datetime', 'code', 'open', 'high', 'low', 'close', 'volume']
        if len(code) == 3:
            history_bar = []
            con = str(start.year)[-2:] + (str(start.month) if start.month > 9 else '0' + str(start.month))
            _start = start
            for _s in CODE_SUFFIX:
                if _s >= con:
                    expiry = self._db.future_contract_info.find_one({'CODE': code + _s})['EXPIRY_DATE']
                    if expiry <= start:
                        continue

                    if expiry <= end:
                        print(code+_s, _start, expiry)
                        cursor = self._col.find({'code': code + _s, 'datetime': {'$gte': _start, '$lt': expiry}})
                        history_bar.extend([d for d in cursor])
                        _start = expiry
                    else:
                        print(code + _s, _start, expiry)
                        expiry = end
                        cursor = self._col.find({'code': code + _s, 'datetime': {'$gte': _start, '$lt': expiry}})
                        history_bar.extend([d for d in cursor])
                        break
        else:
            cursor = self._col.find({'code': code, 'datetime':{'$gte': start if start is not None else dt.datetime(1970, 1, 1),
                                                               '$lt': end if end is not None else dt.datetime(2999, 1, 1)}}, _fields)
            history_bar = [d for d in cursor]   # 从本地服务器获取历史数据
        df = pd.DataFrame(history_bar, columns=_fields)
        df.set_index('datetime', drop=False, inplace=True)
        apply_func_dict = {'datetime': 'last',
                           'code': 'first',
                           'open': 'first',
                           'high': 'max',
                           'low': 'min',
                           'close': 'last',
                           'volume': 'sum',
                               }

        resampled_df = df.resample(ktype).apply(apply_func_dict)
        resampled_df.dropna(how='all', inplace=True)
        if fields is None:
            fields = [field for field in resampled_df.columns]
        else:
            fields = [field for field in fields if field in resampled_df.columns]

        return resampled_df.loc[:, fields]

    def get_available_contract(self, underlying:str, date):
        if isinstance(date, str):
            _date = parser.parse(date).replace(hour=0, minute=0, second=0)
        elif isinstance(date, dt.datetime):
            _date = date.replace(hour=0, minute=0, second=0)
        else:
            raise ValueError('请输入str或者datetime类型')

        underlying = underlying.upper()

        contract_info = pd.DataFrame([ci for ci in self._db.get_collection('future_contract_info').find({'CLASS_CODE': underlying, 'DATE': _date})])

        return contract_info

    def get_main_contract_bars(self, underlying, fields=None, start=None, end=None, ktype='1m'):
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        ktype = _check_ktype(ktype)
        _fields = ['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'date_stamp']
        _d = start.replace(hour=0, minute=0, second=0)
        _end = end.replace(hour=0, minute=0, second=0)
        data = []
        while _d <= _end:
            _c_info = self.get_available_contract(underlying, _d)
            if _c_info.empty:
                _d += dt.timedelta(days=1)
                continue

            if _c_info.loc[0, 'EXPIRY_DATE'] - _d > dt.timedelta(days=2):
                code = _c_info.loc[0, 'CODE']
            else:
                code = _c_info.loc[1, 'CODE']

            d = [ret for ret in self._col.find(
                {'code': code, 'datetime': {'$gte': _d.replace(hour=9, minute=14, second=0), '$lt': _d.replace(hour=17, minute=0, second=0)}}, _fields, sort=[('datetime', pmg.DESCENDING)])]

            _bar_before_d = self._col.find_one({'code': code,  'datetime': {'$lt': _d.replace(hour=9, minute=14, second=0)}},
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
            data.extend(d)
            _d += dt.timedelta(days=1)

        df = pd.DataFrame(data, columns=_fields)
        df.set_index('datetime', drop=False, inplace=True)
        apply_func_dict = {'datetime': 'last',
                           'code': 'first',
                           'open': 'first',
                           'high': 'max',
                           'low': 'min',
                           'close': 'last',
                           'volume': 'sum',
                           }

        resampled_df = df.resample(ktype).apply(apply_func_dict)
        resampled_df.dropna(how='all', inplace=True)
        if fields is None:
            fields = [field for field in resampled_df.columns]
        else:
            fields = [field for field in fields if field in resampled_df.columns]

        return resampled_df.loc[:, fields]