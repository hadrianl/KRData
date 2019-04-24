#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/8 0008 11:04
# @Author  : Hadrianl 
# @File    : IBData

import pymongo as pmg
import datetime as dt
import pandas as pd
from dateutil import parser
from mongoengine import *
from ib_insync import *

class IBData:
    def __init__(self, username, password, host='192.168.2.226', port=27017):
        self.cli = pmg.MongoClient(host, port)
        self.cli.get_database('admin').authenticate(username,password)
        self.db = self.cli.get_database('IB')

    def get_trade_records(self, contract=None, start=None, end=None, convert_df=True):
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        start = start if start is not None else dt.datetime(1970, 1, 1)
        end = end if end is not None else dt.datetime(2050, 1, 1)
        _filter = {'time': {'$gte': start, '$lte': end}}

        if contract:
            _filter.update({'contract.localSymbol': contract})

        col = self.db.get_collection('Trade')
        cur = col.find(_filter)
        raw_data = [t for t in cur]
        if convert_df:
            essential_data = [[d['time'], d['contract']['localSymbol'], d['execution']['execId'], d['execution']['side'],  d['execution']['price'], d['execution']['shares'], d['commissionReport']['commission'] ] for d in raw_data]
            data = pd.DataFrame(essential_data, columns=['datetime', 'symbol', 'execId', 'side', 'price', 'qty', 'commission'])
            return data
        else:
            return raw_data

class IBMarket:
    def __init__(self):
        self.ib = IB()
        self.MkData = IBMarketData

    def connectDB(self, username, password, host='192.168.2.226', port=27017):
        register_connection('IBMarket', db='IBMarket', host=host, port=port, username=username, password=password, authentication_source='admin')

    def connectIB(self, host='127.0.0.1', port=7497, clientId=0):
        self.ib.connect(host, port, clientId)

    def update_mkData_from_IB(self, contract: Contract):
        contract,  = self.ib.qualifyContracts(contract)
        data = self.__getitem__(contract)
        if data.count() == 0:
            headTimestamp = self.ib.reqHeadTimeStamp(contract, 'TRADES', useRTH=False)
            latest_datetime = headTimestamp
        else:
            latest_datetime = data.order_by('-datetime').first().datetime
        delta = dt.datetime.now() - latest_datetime
        total_seconds = delta.total_seconds()
        if total_seconds <= 86400:
            mkdata = self.ib.reqHistoricalData(contract, '', f'{delta.total_seconds() + 60} S', '1 min', 'TRADES',
                                               useRTH=False, keepUpToDate=False)
        elif total_seconds <= 86400 * 30:
            mkdata = self.ib.reqHistoricalData(contract, '', f'{min(delta.days + 1, 30)} D', '1 min', 'TRADES', useRTH=False, keepUpToDate=False)
        elif total_seconds < 86400 * 30 * 6:
            mkdata = self.ib.reqHistoricalData(contract, '', f'{min(delta.days // 30 + 1, 6)} M', '1 min', 'TRADES', useRTH=False, keepUpToDate=False)
        else:
            mkdata = self.ib.reqHistoricalData(contract, '', f'{delta.days // 30 * 12 + 1} Y', '1 min', 'TRADES', useRTH=False, keepUpToDate=False)
        for bar in mkdata:
            d = self.MkData.from_ibObject(contract, bar)
            try:
                d.save()
            except NotUniqueError:
                continue
            except Exception as e:
                raise e

    def get_bars(self, contract, start=None, end=None, exclude_contract=True):
        bar = self.__getitem__(contract)
        exclude = ['id']
        if exclude_contract:
            exclude.append('contract')
        filter = {}
        if start is not None:
            filter['datetime__gte'] = start

        if end is not None:
            filter['datetime__lte'] = start

        raw_object = bar.exclude(*exclude).filter(**filter)

        df = pd.DataFrame([r for r in raw_object.values_list('datetime', 'open', 'high', 'low', 'close', 'volume', 'barCount', 'average')], columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'barCount', 'average']).set_index('datetime', drop=False)

        return df

    def __getitem__(self, contract: Contract):
        return self.MkData.objects(contract__conId=contract.conId)


class IBContract(EmbeddedDocument):
    secType = StringField()
    conId = IntField(required=True)
    symbol = StringField()
    lastTradeDateOrContractMonth = StringField()
    strike = FloatField()
    right = StringField()
    multiplier = StringField()
    exchange = StringField()
    primaryExchange = StringField()
    currency = StringField()
    localSymbol = StringField()
    tradingClass = StringField()
    includeExpired = BooleanField()
    secIdType = StringField()
    secId = StringField()

    meta = {'db_alias': 'IBMarket', 'collection': 'marketData_1min'}

    @staticmethod
    def from_ibObject(contract):
        c = IBContract()
        c.secType = contract.secType
        if contract.conId != 0:
            c.conId = contract.conId
        else:
            raise ValueError("conId不能为0，请确认contract")
        c.symbol = contract.symbol
        c.lastTradeDateOrContractMonth = contract.lastTradeDateOrContractMonth
        c.strike = contract.strike
        c.right = contract.right
        c.multiplier = contract.multiplier
        c.exchange = contract.exchange
        c.primaryExchange = contract.primaryExchange
        c.currency = contract.currency
        c.localSymbol = contract.localSymbol
        c.tradingClass = contract.tradingClass
        c.includeExpired = contract.includeExpired
        c.secIdType = contract.secIdType
        c.secId = contract.secId

        return c

class IBMarketData(Document):
    contract = EmbeddedDocumentField(IBContract, required=True)
    datetime = DateTimeField(required=True, unique_with='contract.conId')
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = IntField()
    barCount = IntField()
    average = FloatField()

    meta = {'db_alias': 'IBMarket', 'collection': 'marketData_1min'}

    @staticmethod
    def from_ibObject(contract, barData):
        md = IBMarketData()
        md.contract = IBContract.from_ibObject(contract)
        md.datetime = barData.date
        md.open = barData.open
        md.high = barData.high
        md.low = barData.low
        md.close = barData.close
        md.volume = barData.volume
        md.barCount = barData.barCount
        md.average = barData.average

        return md


if __name__ == '__main__':
    ib = IBData('username', 'password')
    data = ib.get_trade_records()
    print(data)