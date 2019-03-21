#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/8 0008 11:04
# @Author  : Hadrianl 
# @File    : IBData

import pymongo as pmg
import datetime as dt
import pandas as pd
from dateutil import parser

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


if __name__ == '__main__':
    ib = IBData('username', 'password')
    data = ib.get_trade_records()
    print(data)