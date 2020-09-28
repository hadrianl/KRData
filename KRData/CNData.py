#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/9/24 0024 13:54
# @Author  : Hadrianl 
# @File    : CNData

from mongoengine import connect, Document, FloatField, StringField, IntField, QuerySet
from typing import Union, List
import datetime as dt
from dateutil import parser
import pandas as pd

class CNMarketData(Document):
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    vol = FloatField()
    amount = FloatField()
    date = StringField(required=True)
    code = StringField(required=True)
    date_stamp = IntField()

    meta = {'db_alias': 'CNMarket', 'collection': 'stock_day'}

class CNMarketData_ADJ(Document):
    date = StringField()
    code = StringField()
    adj = FloatField()

    meta = {'db_alias': 'CNMarket', 'collection': 'stock_adj'}

class CNMarketData_XDXR(Document):
    category = IntField()
    name = StringField()
    fenhong = FloatField()
    peigujia = FloatField()
    songzhuangu = FloatField()
    peigu = FloatField()
    suogu = FloatField()
    liquidity_before = FloatField()
    liquidity_after = FloatField()
    shares_before = FloatField()
    shares_after = FloatField()
    fenshu = FloatField()
    xingquanjia = FloatField()
    date = StringField()
    category_meaning = StringField()
    code = StringField()

    meta = {'db_alias': 'CNMarket', 'collection': 'stock_xdxr'}

class CNMarket:
    def __init__(self):
        self._MarketData = CNMarketData
        connect(alias='CNMarket', db='quantaxis', host='192.168.2.226', port=27016)

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
                    start = parser.parse(item.start).timestamp()
                    params['date_stamp__gte'] = start
                elif isinstance(item.start, dt.datetime):
                    start = item.start.timestamp()
                    params['date_stamp__gte'] = start
                elif isinstance(item.start, int):
                    backward = item.start

            if item.stop:
                if isinstance(item.stop, str):
                    stop = parser.parse(item.stop).timestamp()
                    params['date_stamp__lt'] = stop
                elif isinstance(item.stop, dt.datetime):
                    stop = item.stop.timestamp()
                    params['date_stamp__lt'] = stop
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

            return query_set
        else:
            raise Exception(f'item类型应为{Union[str, slice, List]}')

        return query_set

    @staticmethod
    def to_df(query_set: QuerySet, fq=None) -> pd.DataFrame:
        cols = ['code', 'date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'date_stamp']
        data = pd.DataFrame(query_set.values_list(*cols), columns=cols)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index(['code', 'date'], drop=False)

        if fq:

            codes = data['code'].unique()
            _info = CNMarketData_XDXR.objects(code__in=codes, category=1)
            info_cols = ['code', 'date', 'category', 'fenhong', 'peigu', 'peigujia', 'songzhuangu']
            if _info.count() > 0:
                info = pd.DataFrame(_info.values_list(*info_cols), columns=info_cols)

                data = data.assign(is_trade=1)
                info['date'] = pd.to_datetime(info['date'])
                info = info.set_index(['code', 'date'])

                data_ = pd.concat([data, info], axis=1)

                def calc_fq(df):
                    df[cols].ffill(inplace=True)
                    df.dropna(subset=cols, inplace=True)
                    df.fillna(0, inplace=True)

                    df['preclose'] = (df['close'].shift(1) * 10 - df['fenhong'] +
                                         df['peigu'] * df['peigujia']) \
                                        / (10 + df['peigu'] + df['songzhuangu'])

                    if fq == 'qfq':
                        df['adj'] = (df['preclose'].shift(-1) / df['close']).fillna(1)[::-1].cumprod()
                    else:
                        df['adj'] = (df['close'] / df['preclose'].shift(-1)).cumprod().shift(1).fillna(1)

                    df[['open', 'high', 'low', 'close', 'preclose']] = df[['open', 'high', 'low', 'close', 'preclose']].mul(df['adj'], axis=0)

                    return df

                data_ = data_.groupby(level='code').apply(calc_fq).droplevel(0)

                data = data_[data_['is_trade'] == 1].drop(['is_trade', 'category', 'fenhong', 'peigu', 'peigujia', 'songzhuangu',  'preclose', 'adj'], axis=1)

        return data