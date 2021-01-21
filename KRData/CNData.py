#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/9/24 0024 13:54
# @Author  : Hadrianl 
# @File    : CNData

from mongoengine import connect, Document, FloatField, StringField, IntField, DateTimeField, QuerySet
from typing import Union, List
import datetime as dt
from dateutil import parser
import pandas as pd
from .util import load_json_settings

class CNStock_Daily_OHLCV(Document):
    code = StringField(required=True)
    datetime = DateTimeField(required=True, unique_with='code')
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = FloatField()
    amount = FloatField()
    outstanding_share = FloatField()
    turnover = FloatField()
    after_volume = FloatField()
    after_amount = FloatField()


    meta = {'db_alias': 'CNStock',
            'collection': 'stock_ohlcv',
            'indexes': [
                'code',
                {
                    'fields': ('code', 'datetime'),
                    'unique': True
                },
            ]
            }

class CNStock_Daily_HFQ_Factor(Document):
    code = StringField(required=True)
    datetime = DateTimeField(required=True, unique_with='code')
    factor = FloatField()

    meta = {'db_alias': 'CNStock',
            'collection': 'stock_hfq_factor',
            'indexes': [
                'code',
                {
                    'fields': ('code', 'datetime'),
                    'unique': True
                },
            ]
            }

class CNStock_Daily_QFQ_Factor(Document):
    code = StringField(required=True)
    datetime = DateTimeField(required=True, unique_with='code')
    factor = FloatField()

    meta = {'db_alias': 'CNStock',
            'collection': 'stock_qfq_factor',
            'indexes': [
                'code',
                {
                    'fields': ('code', 'datetime'),
                    'unique': True
                },
            ]
            }

class CNStock_Info_Code_Name(Document):
    code = StringField(required=True, unique=True)
    name = StringField()
    market = StringField()

    meta = {'db_alias': 'CNStock',
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


class CNStock:
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
        self._MarketData = CNStock_Daily_OHLCV
        self._data_type = data_type
        self._adjust = adjust
        connect(alias='CNStock', db='CNStock', host=config['host'], port=config['port'], username=config['user'],
                password=config['password'], authentication_source='admin')

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
        cols = ['code', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'outstanding_share', 'turnover']
        data = pd.DataFrame(query_set.values_list(*cols), columns=cols)
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
        hfq_factor_qs = CNStock_Daily_HFQ_Factor.objects(code__in=codes)
        hfq_col = ['datetime', 'code', 'factor']
        hfq_factor = pd.DataFrame(hfq_factor_qs.values_list(*hfq_col), columns=hfq_col).set_index(['code', 'datetime']).sort_index(level=['code', 'datetime'])

        def hfq(df):
            code = df.index[0][0]
            factor = hfq_factor.loc[code].asof(df['datetime']).values
            factor = factor / factor[0]
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].mul(factor)

            return df

        data = data.groupby(level='code').apply(hfq)

        return data

    @staticmethod
    def _qfq(data):
        codes = data['code'].unique()
        qfq_factor_qs = CNStock_Daily_QFQ_Factor.objects(code__in=codes)
        qfq_col = ['datetime', 'code', 'factor']
        qfq_factor = pd.DataFrame(qfq_factor_qs.values_list(*qfq_col), columns=qfq_col).set_index(['code', 'datetime']).sort_index(level=['code', 'datetime'])

        def qfq(df):
            code = df.index[0][0]
            factor = qfq_factor.loc[code].asof(df['datetime']).values
            factor = factor / factor[-1]
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].div(factor)

            return df

        data = data.groupby(level='code').apply(qfq)

        return data



def update(config=None):
    import akshare as ak
    import pandas as pd
    import traceback
    import time

    today = dt.date.today()
    today_weekday = today.isoweekday()
    if today_weekday in [1, 7]:
        print(f'{today} has no update!')
        return

    config = config or load_json_settings('mongodb_settings.json')
    if not config:
        raise Exception('请先配置mongodb')

    print(f'connect to {config["host"]}:{config["port"]}')
    client = connect(alias='CNStock', db='CNStock', host=config['host'], port=config['port'], username=config['user'],
                password=config['password'], authentication_source='admin')

    client.get_database('admin').authenticate(name=config['user'], password=config['password'])

    db = client.get_database('CNStock')
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
    big_df = pd.DataFrame()
    stock_sh = ak.stock_info_sh_name_code(indicator="主板A股")
    stock_sh = stock_sh[["SECURITY_CODE_A", "SECURITY_ABBR_A"]]
    stock_sh.columns = ["code", "name"]
    stock_sh["code"] = 'sh' + stock_sh["code"]
    stock_sh["market"] = 'sh'

    stock_sz = ak.stock_info_sz_name_code(indicator="A股列表")
    stock_sz["A股代码"] = stock_sz["A股代码"].astype(str).str.zfill(6)
    stock_sz = stock_sz[["A股代码", "A股简称"]]
    stock_sz.columns = ["code", "name"]
    stock_sz["code"] = 'sz' + stock_sz["code"]
    stock_sz["market"] = 'sz'

    stock_kcb = ak.stock_info_sh_name_code(indicator="科创板")
    stock_kcb = stock_kcb[["SECURITY_CODE_A", "SECURITY_ABBR_A"]]
    stock_kcb.columns = ["code", "name"]
    stock_kcb["code"] = 'sh' + stock_kcb["code"]
    stock_kcb["market"] = 'kcb'

    big_df = big_df.append(stock_sh, ignore_index=True)
    big_df = big_df.append(stock_sz, ignore_index = True)
    big_df = big_df.append(stock_kcb, ignore_index=True)

    CNStock_Info_Code_Name.drop_collection()
    for _, data in big_df.iterrows():
        qs = CNStock_Info_Code_Name(code=data['code'], name=data['name'], market=data['market'])
        qs.save()


    all_info = CNStock_Info_Code_Name.objects()

    print('start update stock data')
    for info in all_info:
        try:
            ohlcv_list = []
            hfq_list = []
            qfq_list = []

            last_day = (col_ohlcv.find_one({'code': info.code}, sort=[('datetime', -1)]) or {}).get('datetime', dt.datetime(1990, 1, 1)) + dt.timedelta(days=1)
            if last_day.date() >= today:
                continue

            if info.market in ['sh', 'sz']:
                data = ak.stock_zh_a_daily(info.code)
                hfq_factor = ak.stock_zh_a_daily(info.code, adjust='hfq-factor')
                qfq_factor = ak.stock_zh_a_daily(info.code, adjust='qfq-factor')
            elif info.market == 'kcb':
                data = ak.stock_zh_kcb_daily(info.code)
                hfq_factor = ak.stock_zh_kcb_daily(info.code, adjust='hfq-factor')
                qfq_factor = ak.stock_zh_kcb_daily(info.code, adjust='qfq-factor')
            else:
                raise Exception("unknown market flag")

            for d, v in data[last_day:].iterrows():
                ohlcv_list.append({'datetime': d.to_pydatetime(), 'code': info.code, **v.to_dict()})

            for d, r in hfq_factor.iterrows():
                hfq_list.append({'datetime': d.to_pydatetime(), 'code': info.code, 'factor': float(r.hfq_factor)})

            for d, r in qfq_factor.iterrows():
                qfq_list.append({'datetime': d.to_pydatetime(), 'code': info.code, 'factor': float(r.qfq_factor)})


            if ohlcv_list:
                col_ohlcv.insert_many(ohlcv_list, ordered=False)
            col_hfq.delete_many({'code': info.code})
            col_hfq.insert_many(hfq_list)
            col_qfq.delete_many({'code': info.code})
            col_qfq.insert_many(qfq_list)

            time.sleep(1)
        # except KeyError:
        #     continue
        except Exception:
            print(f'update {info.code} failed with error:\n {traceback.format_exc()})')
