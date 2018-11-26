#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/21 0021 12:46
# @Author  : Hadrianl 
# @File    : __init__.py

import pymongo as pmg

__version__ = '1.7'


class BaseData:
    def __init__(self, host, port, db, user=None, pwd=None, ):
        self._mongodb_host = host
        self._mongodb_port = port
        self._user = user
        self._conn = pmg.MongoClient(f'mongodb://{host}:{port}/')
        if bool(user)&bool(pwd):
            admin_db = self._conn.get_database('admin')
            admin_db.authenticate(user, pwd)
        self._db = self._conn.get_database(db)
        self._col = self._db.get_collection('future_1min')

    def get_all_codes(self):   # 获取本地合约列表
        code_list = self._col.distinct('code')
        return code_list

    def get_bars(self, code, fields=None, start=None, end=None, ktype='1m'):
        raise NotImplementedError

def entry_point():
    print(f'SUCCEED!VERSION:{__version__}')