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