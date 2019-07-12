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
import numpy as np
import re
from .util import load_json_settings, Singleton, _concat_executions
from functools import lru_cache
import warnings

class IBData:
    def __init__(self):
        config = load_json_settings('mongodb_settings.json')
        if not config:
            raise Exception('请先配置mongodb')
        self.cli = pmg.MongoClient(config['host'], config['port'])
        self.cli.get_database('admin').authenticate(config['user'], config['password'])
        self.db = self.cli.get_database('IB')

    def get_trade_records(self, contract=None, start=None, end=None, account=None, convert_df=True):
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)

        start = start if start is not None else dt.datetime(1970, 1, 1)
        end = end if end is not None else dt.datetime(2050, 1, 1)
        _filter = {'time': {'$gte': start, '$lte': end}}

        if contract:
            _filter.update({'contract.localSymbol': contract})

        if account:
            _filter.update({'execution.acctNumber': account})
        col = self.db.get_collection('Trade')
        cur = col.find(_filter)
        raw_data = [t for t in cur]
        if convert_df:
            essential_data = [[d['time'], d['execution']['acctNumber'],d['contract']['localSymbol'], d['execution']['execId'], d['execution']['side'],  d['execution']['price'], d['execution']['shares'], d['commissionReport']['commission'] ] for d in raw_data]
            data = pd.DataFrame(essential_data, columns=['datetime', 'account', 'symbol', 'execId', 'side', 'price', 'qty', 'commission'])
            return data
        else:
            return raw_data

class IBMarket(metaclass=Singleton):
    def __init__(self):
        self.ib = IB()
        self.MkData = IBMarketData

        mongo_config = load_json_settings('mongodb_settings.json')
        if mongo_config:
            self.connectDB(mongo_config['user'], mongo_config['password'], host=mongo_config['host'], port=mongo_config['port'])
        else:
            warnings.warn('未配置mongodb_settings，需要使用connectDB来连接[IBMarket]')

        ib_mkdata_config = load_json_settings('ib_mkdata_settings.json')
        if ib_mkdata_config:
            self.connectIB(ib_mkdata_config['host'], ib_mkdata_config['port'], ib_mkdata_config['clientId'])
        else:
            warnings.warn('未配置ib_mkdata_settings，需要使用connectDB来连接[IBMarket]')

    def connectDB(self, username, password, host='192.168.2.226', port=27017):
        register_connection('IBMarket', db='IBMarket', host=host, port=port, username=username, password=password, authentication_source='admin')

    def connectIB(self, host='127.0.0.1', port=7497, clientId=18):
        self.ib.connect(host, port, clientId)

    def __del__(self):
        self.ib.disconnect()

    def save_mkData_from_IB(self, contract: (Contract, int, str), start=None, end=None, keepUpToDate=False):
        if not self.ib.isConnected():
            self.connectIB()

        def get_history_bars(_contract):
            if isinstance(_contract, int):
                _contract = Contract(conId=_contract)
            elif isinstance(_contract, str):
                r = re.match(r'([A-Z]+)(\d{2,})', _contract)
                if r:
                    symbol, num = r.groups()
                    _contract = Contract(symbol=symbol, lastTradeDateOrContractMonth=f'20{num}')
                else:
                    raise Exception(f'{_contract}名称有误')
            _contract, = self.ib.qualifyContracts(_contract)
            data = self.__getitem__(_contract)

            if start is None:
                if not data or data.count() == 0:
                    headTimestamp = self.ib.reqHeadTimeStamp(_contract, 'TRADES', useRTH=False)
                    latest_datetime = headTimestamp
                else:
                    latest_datetime = data.order_by('-datetime').first().datetime
            else:
                latest_datetime = start if isinstance(start, dt.datetime) else parser.parse(start)

            _end = '' if end is None else (end if isinstance(end, dt.datetime) else parser.parse(end))
            delta = dt.datetime.now() - latest_datetime if _end == '' else _end - latest_datetime
            total_seconds = delta.total_seconds()

            if total_seconds < 86400:
                mkdata = self.ib.reqHistoricalData(_contract, _end, f'{int(delta.total_seconds() //60 * 60  + 60)} S', '1 min', 'TRADES',
                                                   useRTH=False, keepUpToDate=keepUpToDate)
            elif total_seconds <= 86400 * 30:
                mkdata = self.ib.reqHistoricalData(_contract, _end, f'{int(min(delta.days + 1, 30))} D', '1 min', 'TRADES', useRTH=False, keepUpToDate=keepUpToDate)
            elif total_seconds < 86400 * 30 * 6:
                mkdata = self.ib.reqHistoricalData(_contract, _end, f'{int(min(delta.days // 30 + 1, 6))} M', '1 min', 'TRADES', useRTH=False, keepUpToDate=keepUpToDate)
            else:
                mkdata = self.ib.reqHistoricalData(_contract, _end, f'{int(delta.days // (30 * 12) + 1)} Y', '1 min', 'TRADES', useRTH=False, keepUpToDate=keepUpToDate)

            return mkdata

        def save_bar(contract, bar):
            d = self.MkData.from_ibObject(contract, bar)
            try:
                d.save()
            except NotUniqueError:
                ...
            except Exception as e:
                raise e

        barData = get_history_bars(contract)

        for bar in barData[:-1]:
            save_bar(contract, bar)

        if keepUpToDate:
            def update_bar(bars, hasNewBar):
                if hasNewBar:
                    data = self.__getitem__(contract)
                    l_dt = data.order_by('-datetime').first().datetime
                    for bar in bars[:-1]:
                        if bar.date > l_dt:
                            save_bar(contract, bar)

            barData.updateEvent += update_bar
            for notConnect in self.ib.loopUntil(lambda : not self.ib.isConnected()):
                if notConnect:
                    try:
                        self.connectIB(self.ib.client.host, self.ib.client.port, self.ib.client.clientId)
                        barData = get_history_bars(contract)

                        for bar in barData[:-1]:
                            save_bar(contract, bar)

                        barData.updateEvent += update_bar
                    except Exception as e:
                        print(f'{contract}自动更新异常->{e}！5分钟后重连更新')
                        now = dt.datetime.now()
                        util.waitUntil(now + dt.timedelta(minutes=5))
                        lastTradeDate = parser.parse(contract.lastTradeDateOrContractMonth)
                        if now - lastTradeDate > dt.timedelta(days=1):  # 合约已经到期之后自动换合约，仅针对恒指
                            contract = Contract(contract.symbol, (lastTradeDate + dt.timedelta(weeks=4)).strftime('%Y%m'))

    @lru_cache(maxsize=30)
    def get_bars(self, contract: (Contract, str, int), start=None, end=None):
        if isinstance(contract, (Contract, str)):
            contract, = self.verifyContract(contract)
        bar = self.__getitem__(contract)

        filter = {}
        if start is not None:
            filter['datetime__gte'] = start

        if end is not None:
            filter['datetime__lte'] = end

        raw_object = bar.filter(**filter)

        return self.to_df(raw_object)

    @lru_cache(maxsize=30)
    def get_bars_from_ib(self, contract: (Contract, str, int), barType='1 min', start=None, end=None, persist=False):
        _contract = self.verifyContract(contract)
        if start is None:
            start = self.ib.reqHeadTimeStamp(_contract, 'TRADES', useRTH=False)
        else:
            start = start if isinstance(start, dt.datetime) else parser.parse(start)

        if end is None:
            end = ''
        else:
            end = end if isinstance(end, dt.datetime) else parser.parse(end)

        delta = dt.datetime.now() - start if end == '' else end - start
        total_seconds = delta.total_seconds()
        if total_seconds < 86400:
            barSizeSetting = f'{int(delta.total_seconds() // 60 * 60 + 60)} S'
        elif total_seconds <= 86400 * 30:
            if end and end.time() >= dt.time(17, 15):
                offset = 2
            else:
                offset = 1
            barSizeSetting = f'{int(min(delta.days + offset, 30))} D'
        elif total_seconds < 86400 * 30 * 6:
            barSizeSetting = f'{int(min(delta.days // 30 + 1, 6))} M'
        else:
            barSizeSetting = f'{int(delta.days // (30 * 12) + 1)} Y'

        barlist = self.ib.reqHistoricalData(_contract, end, barSizeSetting, barType,
                                               'TRADES', useRTH=False, keepUpToDate=False)

        if persist:
            for bar in barlist:
                d = self.MkData.from_ibObject(_contract, bar)
                try:
                    d.save()
                except NotUniqueError:
                    ...
                except Exception as e:
                    raise e

        df = util.df(barlist).rename(columns = {'date': 'datetime'}).set_index('datetime', drop=False)[start:]

        return df

    @staticmethod
    def to_df(objects):
        return pd.DataFrame([r for r in objects.values_list('datetime', 'open', 'high', 'low', 'close', 'volume', 'barCount', 'average')], columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'barCount', 'average']).set_index('datetime', drop=False)

    def __getitem__(self, item: (Contract, str, int, slice)):
        if isinstance(item, Contract):
            return self.MkData.objects(contract__conId=contract.conId)
        elif isinstance(item, str):
            r = re.match(r'([A-Z]+)(\d{2,})', item)
            if r:
                symbol, num = r.groups()
                return self.MkData.objects(contract__symbol=symbol, contract__lastTradeDateOrContractMonth__contains=f'20{num}')
            else:
                return self.MkData.objects(contract__localSymbol=item)
        elif isinstance(item, int):
            return self.MkData.objects(contract__conId=item)
        elif isinstance(item, slice):
            filter_ = {}
            if isinstance(item.start, dt.datetime):
                filter_['datetime__gte'] = item.start - dt.timedelta(hours=8)
            elif isinstance(item.start, str):
                filter_['datetime__gte'] = parser.parse(item.start) - dt.timedelta(hours=8)

            if isinstance(item.stop, dt.datetime):
                filter_['datetime__lte'] = item.stop - dt.timedelta(hours=8)
            elif isinstance(item.stop, str):
                filter_['datetime__lte'] = parser.parse(item.stop) - dt.timedelta(hours=8)

            if isinstance(item.step, str):
                r = re.match(r'([A-Z]+)(\d{2,})', item.step)
                if r:
                    symbol, num = r.groups()
                    filter_['contract__lastTradeDateOrContractMonth__contains'] = f'20{num}'
                else:
                    filter_['contract__localSymbol'] = item.step
            elif isinstance(item.step, Contract):
                filter_['contract__conId'] = item.step.conId
            elif isinstance(item.step, int):
                filter_['contract__conId'] = item.step

            return self.MkData.objects(**filter_)

    def verifyContract(self, contract: (Contract, str, int)) -> Contract:
        if isinstance(contract, int):
            contract = Contract(conId=contract)
        elif isinstance(contract, str):
            r = re.match(r'([A-Z]+)(\d{2,})', contract)
            if r:
                symbol, num = r.groups()
                contract = Future(symbol=symbol, lastTradeDateOrContractMonth=f'20{num}')
            else:
                raise Exception(f'{contract}名称有误')

        contract = self.ib.qualifyContracts(contract)
        if not contract:
            raise Exception(f'验证{contract}错误')
        return contract[0]



class IBTrade(metaclass=Singleton):
    def __init__(self, account=None):
        self.ib = IB()
        self.account = account
        self.IBFill = IBFill
        self._objects = None
        self._ib_market = IBMarket()

        mongo_config = load_json_settings('mongodb_settings.json')
        if mongo_config:
            self.connectDB(mongo_config['user'], mongo_config['password'], host=mongo_config['host'], port=mongo_config['port'])
        else:
            warnings.warn('未配置mongodb_settings，需要使用connectDB来连接[IBTrade]')

        ib_config = load_json_settings('ib_settings.json')
        if ib_config:
            self.connectIB(ib_config['host'], ib_config['port'], ib_config['clientId'])
        else:
            warnings.warn('未配置ib_settings，需要使用connectDB来连接[IBMarket]')

    def connectDB(self, username, password, host='192.168.2.226', port=27017):
        register_connection('IB', db='IB', host=host, port=port, username=username, password=password, authentication_source='admin')

        if self.account is not None:
            self._objects = self.IBFill.objects(execution__acctNumber=self.account)
        else:
            self._objects = self.IBFill.objects

    def connectIB(self, host='127.0.0.1', port=7497, clientId=0):
        self.ib.connect(host, port, clientId)


    def __del__(self):
        self.ib.disconnect()

    def save_fill_from_IB(self):
        if not self.ib.isConnected():
            raise ConnectionError('请先连接IB->connectIB')

        fills = self.ib.fills()

        saved_fills = []
        for fill in fills:
            f = IBFill.from_ibObject(fill)
            try:
                saved_fills.append(f.save())
            except NotUniqueError:
                continue
            except Exception as e:
                raise e

        return saved_fills

    def display_trades(self, fills, period=1, expand_offset=60, mkdata_source='IB', *, annotate=False, to_file=None):
        """

        :param fills: QuerySet或者Dataframe
        :param period: [1, 5, 15, 30, 60]
        :param expand_offset:
        :param mkdata_source:
        :param annotate:
        :param to_file:
        :return:
        """

        assert period in [1, 5, 15, 30, 60], 'period必须为[1, 5, 15, 30, 60]中的其中一个'

        from .util import draw_klines

        if isinstance(fills, QuerySet):
            conIds = [f.contract.conId for f in fills]

            if len(conIds) == 0:
                raise Exception('没有交易数据')
            elif len(set(conIds)) > 1:
                raise Exception('存在多个合约')

            fills = fills.order_by('execution.time')

            contract = Contract(conId=conIds[0])
            start = fills[0].execution.time - dt.timedelta(minutes=expand_offset) + dt.timedelta(hours=8)
            end = fills[fills.count() - 1].execution.time + dt.timedelta(minutes=expand_offset) + dt.timedelta(hours=8)

            executions = [{'datetime': f.execution.time + dt.timedelta(hours=8), 'price': f.execution.price,
                  'size': f.execution.shares, 'direction': 'long' if f.execution.side == 'BOT' else 'short'} for f in
                 fills]

        elif isinstance(fills, pd.DataFrame):
            conIds = fills.conId.unique()

            if len(conIds) == 0:
                raise Exception('没有交易数据')
            elif len(set(conIds)) > 1:
                raise Exception('存在多个合约')

            fills.sort_index(inplace=True)
            contract = Contract(conId=conIds[0])
            start = fills.iloc[0]['datetime'] - dt.timedelta(minutes=expand_offset)
            end = fills.iloc[-1]['datetime'] + dt.timedelta(minutes=expand_offset)

            executions = fills[['datetime', 'price', 'size', 'direction']].set_index('datetime')
            executions['direction'] = np.where(executions['direction'] == 'BOT', 'long', 'short')

        if mkdata_source == 'IB':
            barTypeMap = {1: '1 min', 5: '5 mins', 15: '15 mins', 30: '30 mins', 60: '1 hour'}

            mkdata = self._ib_market.get_bars_from_ib(contract, barType=barTypeMap.get(period, '1 hour'), start=start, end=end)
        elif mkdata_source == 'HK':
            from .HKData import HKFuture
            hf = HKFuture()
            symbol = fills[0].contract.symbol + fills[0].contract.lastTradeDateOrContractMonth[2:6]
            mkdata = hf.get_bars(symbol, start=start, end=end, ktype=f'{period}min', queryByDate=False)

        mkdata = _concat_executions(mkdata, executions)

        import talib
        mkdata['ma5'] = talib.MA(mkdata['close'].values, timeperiod=5)
        mkdata['ma10'] = talib.MA(mkdata['close'].values, timeperiod=10)
        mkdata['ma30'] = talib.MA(mkdata['close'].values, timeperiod=30)
        mkdata['ma60'] = talib.MA(mkdata['close'].values, timeperiod=60)

        return draw_klines(mkdata, annotate=annotate, to_file=to_file)

    def __getitem__(self, item: (Contract, str, slice)):
        if isinstance(item, Contract):
            return self._objects(contract__conId=item.conId)
        elif isinstance(item, str):
            r = re.match(r'([A-Z]+)(\d{2,})', item)
            if r:
                symbol, num = r.groups()
                return self._objects(contract__symbol=symbol, contract__lastTradeDateOrContractMonth__contains=f'20{num}')
            else:
                return self._objects(contract__localSymbol=item)
        elif isinstance(item, int):
            return self._objects(contract__conId=item)
        elif isinstance(item, slice):
            filter_ = {}
            if isinstance(item.start, dt.datetime):
                filter_['time__gte'] = item.start - dt.timedelta(hours=8)
            elif isinstance(item.start, str):
                filter_['time__gte'] = parser.parse(item.start) - dt.timedelta(hours=8)

            if isinstance(item.stop, dt.datetime):
                filter_['time__lte'] = item.stop - dt.timedelta(hours=8)
            elif isinstance(item.stop, str):
                filter_['time__lte'] = parser.parse(item.stop) - dt.timedelta(hours=8)

            if isinstance(item.step, str):
                r = re.match(r'([A-Z]+)(\d{2,})', item.step)
                if r:
                    symbol, num = r.groups()
                    filter_['contract__lastTradeDateOrContractMonth__contains'] = f'20{num}'
                else:
                    filter_['contract__localSymbol'] = item.step
            elif isinstance(item.step, Contract):
                filter_['contract__conId'] = item.step.conId
            elif isinstance(item.step, int):
                filter_['contract__conId'] = item.step

            return self._objects(**filter_)
        else:
            raise IndexError(f"请检查索引类型")

    def __call__(self, q_obj=None, class_check=True, read_preference=None, **query):
        return self._objects(q_obj=None, class_check=True, read_preference=None, **query)

    @staticmethod
    def to_df(objects):
        df=pd.DataFrame([[o.time,
                          o.contract.localSymbol,
                          o.contract.conId,
                          o.contract.lastTradeDateOrContractMonth,
                          o.execution.execId,
                          o.execution.permId,
                          o.execution.clientId,
                          o.execution.acctNumber,
                          o.execution.side,
                          o.execution.shares,
                          o.execution.price,
                          o.execution.orderRef,
                          o.commissionReport.commission,
                          o.commissionReport.currency] for o in objects],
                         columns=['datetime', 'localSymbol', 'conId', 'expiry', 'execId','permId', 'clientId', 'account',
                                  'direction', 'size', 'price', 'orderRef',
                                  'commission', 'currency'])
        df['datetime'] = df['datetime'] + pd.Timedelta(8, 'hours')
        df = df.set_index('datetime', drop=False)
        return df




# mongoengine document

class IBComboLeg(EmbeddedDocument):
    conId = IntField()
    ratio = IntField()
    action = StringField()
    exchange = StringField()
    openclose = StringField()
    shortSaleSlot = IntField()
    designatedLocation = StringField()
    exemptCode = IntField()

class IBDeltaNeutralContract(EmbeddedDocument):
    condID = IntField()
    delta = FloatField()
    price = FloatField()

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
    comboLegsDescrip = StringField()
    comboLegs = EmbeddedDocumentListField(IBComboLeg)
    deltaNeutralContract = EmbeddedDocumentField(IBDeltaNeutralContract)


    @staticmethod
    def from_ibObject(contract: Contract, extra=False):
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

        if extra:
            c.comboLegsDescrip = contract.comboLegsDescrip

            if contract.comboLegs is not None:
                comboLegList = []
                for comboLeg in contract.comboLegs:
                    cl = IBComboLeg()
                    for k, v in comboLeg.diff().items():
                        setattr(cl, k, v)
                    comboLegList.append(cl)

                c.comboLegs = comboLegList

            if contract.deltaNeutralContract is not None:
                deltaNeutralContract = IBDeltaNeutralContract()
                for k, v in contract.deltaNeutralContract.diff().items():
                    setattr(deltaNeutralContract, k, v)

                c.deltaNeutralContract = deltaNeutralContract

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

class IBExecution(EmbeddedDocument):
    execId = StringField(required=True)
    time = DateTimeField()
    acctNumber = StringField()
    exchange = StringField()
    side = StringField()
    shares = FloatField()
    price = FloatField()
    permId = IntField()
    clientId = IntField()
    orderId = IntField()
    liquidation = IntField()
    cumQty = FloatField()
    avgPrice = FloatField()
    orderRef = StringField()
    evRule = StringField()
    evMultiplier = FloatField()
    modelCode = StringField()
    lastLiquidity = IntField()

    @staticmethod
    def from_ibObject(execution: Execution):
        e = IBExecution()
        for k, v in execution.dict().items():
            setattr(e, k, v)

        return e

class IBCommissionReport(EmbeddedDocument):
    execId = StringField(required=True)
    commission = FloatField()
    currency = StringField()
    realizedPNL = FloatField()
    yield_ = FloatField()
    yieldRedemptionDate = IntField()

    @staticmethod
    def from_ibObject(commissionReport: CommissionReport):
        c = IBCommissionReport()
        for k, v in commissionReport.dict().items():
            setattr(c, k, v)

        return c

class IBFill(Document):
    time = DateTimeField(required=True)
    contract = EmbeddedDocumentField(IBContract, required=True)
    execution = EmbeddedDocumentField(IBExecution, required=True, unique="execution.execId")
    commissionReport = EmbeddedDocumentField(IBCommissionReport)

    meta = {'db_alias': 'IB', 'collection': 'Trade'}

    @staticmethod
    def from_ibObject(fill: Fill):
        f = IBFill()
        f.time = fill.time
        f.contract = IBContract.from_ibObject(fill.contract, True)
        f.execution = IBExecution.from_ibObject(fill.execution)
        f.commissionReport = IBCommissionReport.from_ibObject(fill.commissionReport)
        return f

if __name__ == '__main__':
    im = IBMarket()
    im.connectDB('', '')
    contract = Future('HSI', '201904')
    im.update_mkData_from_IB(contract, True)