#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/21 0021 12:49
# @Author  : Hadrianl 
# @File    : HKData


import pandas as pd
from dateutil import parser
import datetime as dt
from . import BaseData
from .util import _check_ktype, load_json_settings, _concat_executions
import pymongo as pmg
from collections import OrderedDict
from typing import Dict, List, Union
import re


class HKFuture(BaseData):
    def __init__(self, db='HKFuture'):
        config = load_json_settings('mongodb_settings.json')
        if not config:
            raise Exception('请先配置mongodb')
        super(HKFuture, self).__init__(config['host'], config['port'], db, user=config['user'], pwd=config['password'])

    def get_bars(self, code:str, fields:list=None, start:(str, dt.datetime)=None, end:(str, dt.datetime)=None, bar_counts:int=None, ktype:str='1min', queryByDate=True) -> pd.DataFrame:
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
        elif queryByDate:
            ret = self.__get_bars_by_daterange(code, start, end, ktype, fields)
        else:
            ret = self.__get_bars_by_timerange(code, start, end, ktype, fields)  # 根据时间范围来查询

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

    def get_main_contract_trade_dates(self,underlying:str, start:(str, dt.datetime)=None, end:(str, dt.datetime)=None):
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        underlying = underlying.upper()
        trade_date = self.get_trading_dates(start, end, underlying=underlying)

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

        return code_daterage

    def display_trades(self, symbol, executions: List[Dict], period=1, expand_offset: (int, tuple)=60, *, annotate=False, to_file=None):
        """
        交易可视化
        :param symbol: 合约代码，如 HSI1903
        :param executions: 执行订单,
            example:
                [{'datetime': '20190307 14:16:00', 'price': 28000, 'size': 1, 'direction': 'long'},
                {'datetime': '20190307 13:01:00', 'price': 28650, 'size': 1, 'direction': 'short'},
                {'datetime': '20190307 13:01:00', 'price': 28600, 'size': 1, 'direction': 'long'}]
        :param period: [1, 5, 15, 30, 60]
        :param expand_offset: 基于交易执行数据，行情数据前后延伸分钟数，默认60.也可接受(60, 100)前移60，后移100
        :param to_file: 生成图片文件，默认None则不生成
        :return:
        """
        import mpl_finance as mpf
        import talib
        from .util import draw_klines

        assert period in [1, 5, 15, 30, 60], 'period必须为[1, 5, 15, 30, 60]中的其中一个'

        if isinstance(expand_offset, tuple):
            s_offset = expand_offset[0]
            e_offset = expand_offset[1]
        else:
            s_offset = e_offset = expand_offset

        fet = executions[0]['datetime']
        let = executions[0]['datetime']

        start = (fet if isinstance(fet, dt.datetime) else parser.parse(fet)) - dt.timedelta(minutes=s_offset * period)
        end = (let if isinstance(let, dt.datetime) else parser.parse(let)) + dt.timedelta(minutes=e_offset * period)
        market_data = self.get_bars(symbol, start=start, end=end, ktype=f'{period}min', queryByDate=False)

        market_data = _concat_executions(market_data, executions)

        market_data['ma5'] = talib.MA(market_data['close'].values, timeperiod=5)
        market_data['ma10'] = talib.MA(market_data['close'].values, timeperiod=10)
        market_data['ma30'] = talib.MA(market_data['close'].values, timeperiod=30)
        market_data['ma60'] = talib.MA(market_data['close'].values, timeperiod=60)

        return draw_klines(market_data, annotate=annotate, to_file=to_file)

    def __getitem__(self, item: Union[str, slice]):
        if isinstance(item, str):
            return self.get_bars(item, queryByDate=False)
        elif isinstance(item, slice):
            r = re.match(r'([A-Z]+)(\d{2,})', item.step)
            if r:
                return self.get_bars(item.step, start=item.start, end=item.stop, queryByDate=False)
            else:
                return self.get_main_contract_bars(item.step, start=item.start, end=item.stop)


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

    def __get_bars_by_timerange(self, code, start, end, ktype, fields):
        _fields = ['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'trade_date']
        col = self._db.get_collection(f'future_{ktype}_')

        data = [ret for ret in col.find(
            {'code': code, 'datetime': {'$gte': start,
                                        '$lte': end}}, _fields,
            sort=[('datetime', pmg.ASCENDING)])]

        df = pd.DataFrame(data, columns=_fields)
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
            resampled_df.dropna(thresh=2, inplace=True)
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
                elif dt.time(0, 0) < _bar_before_d['datetime'].time() <= dt.time(3, 0):
                    _d_aht = self._col.find({'code': code, 'type': '1min',
                                             'datetime': {'$gte': dt.datetime.fromtimestamp(
                                                 _bar_before_d['date_stamp']) - dt.timedelta(hours=6,
                                                                                             minutes=46),
                                                          '$lte': dt.datetime.fromtimestamp(
                                                              _bar_before_d['date_stamp']) + dt.timedelta(
                                                              hours=3, minutes=0)}},
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