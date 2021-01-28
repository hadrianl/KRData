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
from mongoengine import Document, DateTimeField, FloatField, IntField, StringField, connect, QuerySet, ListField
import pymongo as pmg
from collections import OrderedDict
from typing import Dict, List, Union, Tuple
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
                                            '$lt': td.replace(hour=16, minute=59, second=0)}}, _fields,
                sort=[('datetime', pmg.DESCENDING)])]

            _bar_before_d = self._col.find_one(
                {'code': code, 'datetime': {'$lt': td.replace(hour=9, minute=14, second=0)}},
                _fields,
                sort=[('datetime', pmg.DESCENDING)])

            if _bar_before_d is not None:
                if dt.time(16, 59) < _bar_before_d['datetime'].time() <= dt.time(23, 59):
                    _d_aht = self._col.find({'code': code, 'type': '1min',
                                             'datetime': {'$gte': dt.datetime.fromtimestamp(
                                                 _bar_before_d['date_stamp']) + dt.timedelta(hours=16,
                                                                                             minutes=59),
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
                                                 _bar_before_d['date_stamp']) - dt.timedelta(hours=7,
                                                                                             minutes=1),
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

class HKMarket:
    def __init__(self, period='1min'):
        config = load_json_settings('mongodb_settings.json')
        if not config:
            raise Exception('请先配置mongodb')

        PERIOD = ['1min', '3min', '5min', '15min', '30min', '60min', '1day']
        assert period in PERIOD, f'period must be one of {PERIOD}'

        self._MarketData = type(f'HKMarketData_{period}',
                                (HKMarketDataBaseDocument,),
                                {'meta': {'collection': f'future_{period}_'}})
        connect(alias='HKFuture', db='HKFuture', host=config['host'], port=config['port'], username=config['user'], password=config['password'], authentication_source='admin')

    def __getitem__(self, item: Union[str, slice]) -> QuerySet:
        if isinstance(item, str):
            query_set = self._MarketData.objects(code=item)
        elif isinstance(item, slice):
            date_range_params = {}
            backward = None
            forward = None
            if item.start and isinstance(item.start, (str, dt.datetime)):
                date_range_params['datetime__gte'] = item.start
            elif isinstance(item.start, int):
                backward = item.start

            if item.stop and isinstance(item.stop, (str, dt.datetime)):
                date_range_params['datetime__lt'] = item.stop
            elif isinstance(item.stop, int):
                forward = item.stop

            query_set = self._MarketData.objects(code=item.step, **date_range_params)
            backward = max(query_set.count() - backward, 0) if backward else None
            forward = min(query_set.count(True), forward) if forward else None
            query_set = query_set[backward:forward]

            return query_set
        else:
            raise Exception(f'item类型应为{Union[str, slice]}')

        return query_set

    @staticmethod
    def to_df(query_set: QuerySet, with_trait: List[Tuple]=None) -> pd.DataFrame:
        ohlc = pd.DataFrame(query_set.values_list('datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'trade_date'),
                            columns=['datetime', 'code', 'open', 'high', 'low', 'close', 'volume', 'trade_date']).set_index('datetime', drop=False)

        traits = []
        if with_trait and not ohlc.empty:
            code = ohlc.code[0]
            start = ohlc.datetime[0]
            end = ohlc.datetime[-1]
            for T, p in with_trait:
                ts = T.to_df(T.objects(code=code, datetime__gte=start, datetime__lte=end, param=p))
                traits.append(ts)

        ret = pd.concat([ohlc, *traits], axis=1)

        return ret

class HKMarketDataBaseDocument(Document):
    code = StringField(required=True)
    datetime = DateTimeField(required=True, unique_with='code')
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = IntField()
    trade_date = DateTimeField()

    meta = {'db_alias': 'HKFuture', 'abstract': True,
            'indexes': [
                'code',
                ('code', 'datetime')
            ]}

class HKContractInfo(Document):
    CODE = StringField(required=True)
    CLASS_CODE = StringField()
    EXPIRY_MTH = StringField()
    DATE = DateTimeField(required=True, unique_with='CODE')
    EXPIRY_DATE = DateTimeField()
    CON_SIZE = FloatField()
    DATE_FROM = DateTimeField()
    DATE_TO = DateTimeField()
    Filler = StringField()

    meta = {'db_alias': 'HKFuture', 'collection': 'future_contract_info'}

class HKProductInfo(Document):
    CLASS_CODE = StringField()
    DATE = DateTimeField(required=True, unique_with='CLASS_CODE')
    PROD_NAME = StringField()
    DATE_FROM = DateTimeField()
    DATE_TO = DateTimeField()
    CURRENCY = StringField()
    MULTIPLIER = FloatField()

    meta = {'db_alias': 'HKFuture', 'collection': 'future_product_info'}


# --------------------------------------------HKStock----------------------------------------------------------------
class HKStock_Daily_OHLCV(Document):
    code = StringField(required=True)
    datetime = DateTimeField(required=True, unique_with='code')
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = FloatField()

    meta = {'db_alias': 'HKStock',
            'collection': 'stock_ohlcv',
            'indexes': [
                'code',
                'datetime',
                {
                    'fields': ('code', 'datetime'),
                    'unique': True
                },
            ]
            }

class HKStock_Daily_HFQ_Factor(Document):
    code = StringField(required=True)
    datetime = DateTimeField(required=True, unique_with='code')
    factor = FloatField()
    cash = FloatField()

    meta = {'db_alias': 'HKStock',
            'collection': 'stock_hfq_factor',
            'indexes': [
                'code',
                'datetime',
                {
                    'fields': ('code', 'datetime'),
                    'unique': True
                },
            ]
            }

class HKStock_Daily_QFQ_Factor(Document):
    code = StringField(required=True)
    datetime = DateTimeField(required=True, unique_with='code')
    factor = FloatField()
    cash = FloatField()

    meta = {'db_alias': 'HKStock',
            'collection': 'stock_qfq_factor',
            'indexes': [
                'code',
                'datetime',
                {
                    'fields': ('code', 'datetime'),
                    'unique': True
                },
            ]
            }

class HKStock_Info_Code_Name(Document):
    code = StringField(required=True, unique=True)
    name = StringField()
    engname = StringField()
    market = StringField()

    meta = {'db_alias': 'HKStock',
            'collection': 'stock_code_name',
            'indexes': [
                {
                    'fields': ('code', ),
                    'unique': True,
                },
                'name',
                'market',
            ]
            }


class HKStock:
    def __init__(self, data_type='df', adjust=None):
        """
        daily ohlcv query
        :param data_type: options -> ['raw', 'df'].
        :param adjust: options -> [None, 'qfq', 'hfq']. 'qfq' and 'hfq' work for data_type='df'
        """
        assert data_type in ['df', 'raw']
        assert adjust in ['normal', 'hfq', 'qfq', None]

        config = load_json_settings('mongodb_settings.json')
        if not config:
            raise Exception('请先配置mongodb')
        self._MarketData = HKStock_Daily_OHLCV
        self._data_type = data_type
        self._adjust = adjust
        connect(alias='HKStock', db='HKStock', host=config['host'], port=config['port'], username=config['user'],
                password=config['password'], authentication_source='admin')

    @property
    def all_codes(self):
        return self._MarketData.objects.distinct('code')

    def __getitem__(self, item: Union[str, slice, List]) -> QuerySet:
        if isinstance(item, str):
            query_set = self._MarketData.objects(code=item)
        elif isinstance(item, List):
            query_set = self._MarketData.objects(code__in=item)
        elif isinstance(item, slice):
            params = {}
            backward = None
            forward = None
            if item.start:
                if isinstance(item.start, str):
                    start = parser.parse(item.start)
                    params['datetime__gte'] = start
                elif isinstance(item.start, dt.datetime):
                    start = item.start
                    params['datetime__gte'] = start
                elif isinstance(item.start, int):
                    backward = item.start

            if item.stop:
                if isinstance(item.stop, str):
                    stop = parser.parse(item.stop)
                    params['datetime__lt'] = stop
                elif isinstance(item.stop, dt.datetime):
                    stop = item.stop
                    params['datetime__lt'] = stop
                elif isinstance(item.stop, int):
                    forward = item.stop

            if item.step:
                if isinstance(item.step, str):
                    params['code'] = item.step
                elif isinstance(item.step, List):
                    params['code__in'] = item.step

            query_set = self._MarketData.objects(**params)
            backward = max(query_set.count() - backward, 0) if backward else None
            forward = min(query_set.count(True), forward) if forward else None
            query_set = query_set[backward:forward]

        else:
            raise Exception(f'item类型应为{Union[str, slice, List]}')

        return query_set if self._data_type == 'raw' else self.to_df(query_set)

    def to_df(self, query_set: QuerySet) -> pd.DataFrame:
        cols = ['code', 'datetime', 'open', 'high', 'low', 'close', 'volume']
        data = pd.DataFrame(query_set.as_pymongo())[cols]
        data = data.set_index(['code', 'datetime'], drop=False)

        if not self._adjust:
            return data

        if self._adjust == 'hfq':
            return self._hfq(data)

        if self._adjust == 'qfq':
            return self._qfq(data)

        return data

    @staticmethod
    def _hfq(data):
        codes = data['code'].unique()
        _datetime = data['datetime'].sort_values()
        start = _datetime[0].to_pydatetime()
        end = _datetime[-1].to_pydatetime()
        factor_qs = HKStock_Daily_HFQ_Factor.objects(code__in=codes, datetime__gte=start, datetime__lte=end)
        col = ['datetime', 'code', 'factor', 'cash']
        factor = pd.DataFrame(factor_qs.as_pymongo())[col].set_index(['code', 'datetime']).sort_index(level=['code', 'datetime'])

        def fq(df):
            code = df.index[0][0]
            if code not in factor.index.levels[0]:
                return df

            # ft = factor.loc[code].asof(df['datetime']).values
            _factor = factor.loc[code].asof(df['datetime'])
            ft = _factor['factor']
            cash = _factor['cash']
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].mul(ft, axis=0).add(cash, axis=0)

            return df

        data = data.groupby(level='code').apply(fq)

        return data

    @staticmethod
    def _qfq(data):
        codes = data['code'].unique()
        _datetime = data['datetime'].sort_values()
        start = _datetime[0].to_pydatetime()
        end = _datetime[-1].to_pydatetime()
        factor_qs = HKStock_Daily_QFQ_Factor.objects(code__in=codes, datetime__gte=start, datetime__lte=end)
        col = ['datetime', 'code', 'factor']
        factor = pd.DataFrame(factor_qs.as_pymongo())[col].set_index(['code', 'datetime']).sort_index(level=['code', 'datetime'])

        def fq(df):
            code = df.index[0][0]
            if code not in factor.index.levels[0]:
                return df

            ft = factor.loc[code].asof(df['datetime'])['factor']
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].mul(ft, axis=0)

            return df

        data = data.groupby(level='code').apply(fq)

        return data



def update(config=None):
    import akshare as ak
    import pandas as pd
    import traceback
    import time

    today = dt.date.today()
    today_weekday = today.isoweekday()
    if today_weekday in [6, 7]:
        print(f'{today} has no update!')
        return

    config = config or load_json_settings('mongodb_settings.json')
    if not config:
        raise Exception('请先配置mongodb')

    print(f'connect to {config["host"]}:{config["port"]}')
    client = connect(alias='HKStock', db='HKStock', host=config['host'], port=config['port'], username=config['user'],
                password=config['password'], authentication_source='admin')

    client.get_database('admin').authenticate(name=config['user'], password=config['password'])

    db = client.get_database('HKStock')
    col_ohlcv = db.get_collection('stock_ohlcv')
    col_hfq = db.get_collection('stock_hfq_factor')
    col_qfq = db.get_collection('stock_qfq_factor')

    col_ohlcv.create_index([("code", 1)])
    col_hfq.create_index([("code", 1)])
    col_qfq.create_index([("code", 1)])
    col_ohlcv.create_index([("code", 1), ("datetime", 1)], unique=True)
    col_hfq.create_index([("code", 1), ("datetime", 1)], unique=True)
    col_qfq.create_index([("code", 1), ("datetime", 1)], unique=True)


    print(f'update code info!')
    big_df = ak.stock_hk_spot()[["symbol", "name", "engname"]]
    big_df.columns = ["code", "name", "engname"]
    big_df["market"] = 'hk'

    HKStock_Info_Code_Name.drop_collection()
    for _, data in big_df.iterrows():
        qs = HKStock_Info_Code_Name(code=data['code'], name=data['name'], engname=data['engname'], market=data['market'])
        qs.save()


    all_info = HKStock_Info_Code_Name.objects()

    print('start update stock data')
    for info in all_info:
        try:
            ohlcv_list = []
            hfq_list = []
            qfq_list = []

            last_day = (col_ohlcv.find_one({'code': info.code}, sort=[('datetime', -1)]) or {}).get('datetime', dt.datetime(1970, 1, 1)) + dt.timedelta(days=1)
            if last_day.date() >= today:
                continue


            data = ak.stock_hk_daily(info.code)
            for d, v in data[last_day:].iterrows():
                ohlcv_list.append({'datetime': d.to_pydatetime(), 'code': info.code, **v.to_dict()})

            if ohlcv_list:
                col_ohlcv.insert_many(ohlcv_list, ordered=False)


            hfq_factor = ak.stock_hk_daily(info.code, adjust='hfq-factor')
            for d, r in hfq_factor.iterrows():
                hfq_list.append({'datetime': d.to_pydatetime(), 'code': info.code, 'factor': float(r.hfq_factor)})

            col_hfq.delete_many({'code': info.code})
            col_hfq.insert_many(hfq_list)


            qfq_factor = ak.stock_hk_daily(info.code, adjust='qfq-factor')
            for d, r in qfq_factor.iterrows():
                qfq_list.append({'datetime': d.to_pydatetime(), 'code': info.code, 'factor': float(r.qfq_factor)})

            col_qfq.delete_many({'code': info.code})
            col_qfq.insert_many(qfq_list)

            time.sleep(1)
        # except KeyError:
        #     continue
        except Exception:
            print(f'update {info.code} failed with error:\n {traceback.format_exc()})')