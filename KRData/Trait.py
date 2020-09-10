#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/8/31 0031 13:24
# @Author  : Hadrianl 
# @File    : Trait


from .HKData import HKMarket
from mongoengine import ListField, IntField, StringField, DateTimeField, FloatField, Document, QuerySet
import pandas as pd

class TRAIT(Document):
    name = StringField(required=True)
    code = StringField(required=True)
    datetime = DateTimeField(required=True)
    datas = ListField(FloatField())

    meta = {'allow_inheritance': True, 'db_alias': 'HKFuture', 'collection': 'trait', 'indexes': [{'fields': ('name', 'code', 'datetime')}]}


class MACD(TRAIT):
    param = ListField(IntField(), max_length=3)

    meta = {
        'indexes': [
            {'fields': ('name', 'code', 'datetime', 'param'), 'unique': True}
        ]
    }

    @staticmethod
    def update_trait(code=None, start=None, param=(12, 26, 9)):
        import talib
        hkm = HKMarket()
        _filer = {}
        if code:
            _filer['code'] = code

        if start:
            _filer['datetime__gte'] = start


        codes = hkm._MarketData.objects(**_filer).distinct('code')
        for c in codes:
            df = hkm.to_df(hkm[c])
            macds = talib.MACD(df.close.values, *param)
            macdobs = (MACD(name='macd', code=c, datetime=d, param=param, datas=[v, s, h]) for d, v, s, h in
                       zip(df.datetime, *macds))
            for o in macdobs:
                o.save(validate=False)

    @staticmethod
    def to_df(query_set: QuerySet) -> pd.DataFrame:
        if query_set.count() == 0:
            return pd.DataFrame()

        return pd.DataFrame(
            ((d, *vs) for d, vs in query_set.values_list('datetime', 'datas')),
            columns=['datetime', 'macd', 'macdsignal', 'macdhist']).set_index('datetime')


class MA(TRAIT):
    param = IntField(required=True)
    datas = FloatField(required=True)

    meta = {
        'indexes': [
            {'fields': ('name', 'code', 'datetime', 'param'), 'unique': True}
        ]
    }

    @staticmethod
    def update_trait(code=None, start=None, params=(5, 10, 15, 30, 60)):
        import talib
        hkm = HKMarket()
        _filer = {}
        if code:
            _filer['code'] = code

        if start:
            _filer['datetime__gte'] = start

        codes = hkm._MarketData.objects(**_filer).distinct('code')
        for c in codes:
            df = hkm.to_df(hkm[c])
            for p in params:
                ma = talib.MA(df.close.values, p)
                maobs = (MA(name='ma', code=c, datetime=d, param=p, datas=v) for d, v in zip(df.datetime, ma))
                for o in maobs:
                    o.save(validate=False)
            print(f'{c} succeed')

    @staticmethod
    def to_df(query_set: QuerySet) -> pd.DataFrame:
        if query_set.count() == 0:
            return pd.DataFrame()

        return pd.DataFrame(
            ((d, vs) for d, vs in query_set.values_list('datetime', 'datas')),
            columns=['datetime', f'ma{query_set[0].param}']).set_index('datetime')