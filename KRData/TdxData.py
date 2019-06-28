#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/3 0003 15:43
# @Author  : Hadrianl 
# @File    : TdxData



import pandas as pd
from dateutil import parser
import datetime as dt
from . import BaseData
from .util import _check_ktype, load_json_settings

class TdxFuture(BaseData):
    def __init__(self, db='Future'):
        config = load_json_settings('mongodb_settings.json')
        if not config:
            raise Exception('请先配置mongodb')
        super(TdxFuture, self).__init__(config['host'], config['port'], db, user=config['user'], pwd=config['password'])

    def get_all_codes(self):   # 获取本地合约列表
        code_list = self._col.distinct('code')
        return code_list

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
        _fields = ['datetime', 'open', 'high', 'low', 'close', 'position', 'trade', 'price', 'code', 'market']
        cursor = self._col.find({'code': code, 'datetime':{'$gte': start if start is not None else dt.datetime(1970, 1, 1),
                                                           '$lt': end if end is not None else dt.datetime(2999, 1, 1)}}, _fields)
        history_bar = [d for d in cursor]   # 从本地服务器获取历史数据
        df = pd.DataFrame(history_bar, columns=_fields)
        df.set_index('datetime', drop=False, inplace=True)
        _t = df['datetime'].apply(self.__sort_bars)
        df['_t'] = _t
        df.sort_values('_t', inplace=True)
        def _resample(x):
            _dt = pd.date_range(x.index[0].date(), x.index[0].date() + dt.timedelta(days=1), freq='T')[:len(x)]
            x['_t_temp'] = _dt
            apply_func_dict = {'datetime': 'last',
                               'open': 'first',
                               'high': 'max',
                               'low': 'min',
                               'close': 'last',
                               'position': 'last',
                               'trade': 'sum',
                               'price': 'mean',
                               'code': 'first',
                               'market': 'first',
                               '_t': 'last'
                               }
            resampled = x.resample(ktype, on='_t_temp').apply(apply_func_dict)
            if ktype == '1D':
                resampled.index = resampled['datetime'].apply(lambda x: x.date())
                resampled['datetime'] = resampled['datetime'].apply(lambda x: pd.Timestamp(x.year, x.month, x.day))
            else:
                resampled.set_index('datetime', drop=False, inplace=True)
            return resampled

        resampled_df = df.groupby(by= lambda x: x.date()).apply(_resample)

        if isinstance(resampled_df.index, pd.MultiIndex):
            resampled_df.reset_index(0, drop=True, inplace=True)

        resampled_df = resampled_df.rename(columns={'vol': 'trade'})
        if fields is None:
            fields = [field for field in resampled_df.columns]
        else:
            fields = [field for field in fields if field in resampled_df.columns]

        return resampled_df.loc[:, fields]

    @staticmethod
    def __sort_bars(_dt):
        if _dt.time() > dt.time(18, 0):
            _dt = _dt - dt.timedelta(days=1)
        return _dt.timestamp()

