#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/4 0004 13:07
# @Author  : Hadrianl 
# @File    : IBTickRecorder


from ib_insync import *
import pymongo as pm
import re
from .util import load_json_settings
import warnings

class IBTickRecorder(IB):
    def __init__(self):
        super(IBTickRecorder, self).__init__()

        mongo_config = load_json_settings('mongodb_settings.json')
        if mongo_config:
            self.connectDB(mongo_config['user'], mongo_config['password'], host=mongo_config['host'], port=mongo_config['port'])
        else:
            warnings.warn('未配置mongodb_settings，需要使用connectDB来连接[IBTickRecorder]')

    def connectDB(self, user, pwd, host, port=27017):
        client = pm.MongoClient(host, port)
        auth_db = client.get_database('admin')
        auth_db.authenticate(user, pwd)
        self._db = client.get_database('IBData')

    def connectIB(self, host, port, clientId=9, timeout=10):
        self.connect(host, port, clientId, timeout)

    def RecordTicker(self, contractIDs: list):
        contracts = []
        for contractID in contractIDs:
            r = re.match('([A-Z]+)(\d+)', contractID)
            if r:
                f = Future(r.groups()[0], f'20{r.groups()[1]}')
                contracts.append(f)

        contracts = self.qualifyContracts(*contracts)
        print(f'已确认的合约->{contracts}')

        self.pendingTickersEvent += self.SaveTicker

        for c in contracts:
            self.reqTickByTickData(c, 'AllLast')
            self.reqTickByTickData(c, 'BidAsk')
        print('开启RecordTicker')
        IB.run()

    def SaveTicker(self, pendingTickers):
        for pt in pendingTickers:
            contract = pt.contract.symbol + pt.contract.lastTradeDateOrContractMonth[2:6]
            col_last = self._db.get_collection(f'{contract}_Last')
            col_bidask = self._db.get_collection(f'{contract}_BidAsk')
            for t in pt.tickByTicks:
                if isinstance(t, TickByTickAllLast):
                    data = t._asdict()
                    data['tickAttribLast'] = data['tickAttribLast'].dict()
                    col_last.insert_one(data)
                elif isinstance(t, TickByTickBidAsk):
                    data = t._asdict()
                    data['tickAttribBidAsk'] = data['tickAttribBidAsk'].dict()
                    col_bidask.insert_one(data)

