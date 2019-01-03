#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/2 0002 17:03
# @Author  : Hadrianl 
# @File    : IBTradeRecorder


from ib_insync import *
import pymongo as pm
import click

@click.command()
@click.help_option('-h', '--help')
@click.option('--dbhost', default='192.168.2.226', help='数据库host')
@click.option('--dbport', default=27017, help='数据库port')
@click.option('--user',  default='KRdata', help='数据库用户')
@click.option('--password',  prompt=True, help='数据库密码')
@click.option('--ibhost', default='127.0.0.1', help='IB Host')
@click.option('--ibport', default=7496, help='IB Port')
@click.option('--alive', default=False, type=bool, help='是否持续更新')
def save_ib_trade(dbhost, dbport, user, password, ibhost, ibport, alive):
    ib = IB()
    client = pm.MongoClient(dbhost,dbport)
    auth_db = client.get_database('admin')
    print(user, password)
    auth_db.authenticate(user, password)
    db = client.get_database('IB')
    col = db.get_collection('Trade')
    col.create_index([('time', pm.DESCENDING), ('contract.tradingClass', pm.DESCENDING)])
    col.create_index([('execution.execId', pm.DESCENDING)], unique=True)

    def save_fill(fill):
        f = {'time': fill.time, 'contract': fill.contract.dict(), 'execution': fill.execution.dict(),
             'commissionReport': fill.commissionReport.dict()}
        print('<入库>成交记录:', f)
        col.replace_one({'execution.execId': f['execution']['execId']}, f, upsert=True)

    def save_trade(trade, fill):
        print('<执行更新>')
        if trade.orderStatus.status == 'Filled':
            # t = trade.dict()
            # t['contract'] = t['contract'].dict()
            # t['order'] = t['order'].dict()
            # t['orderStatus'] = t['orderStatus'].dict()
            # t['fills'] = [{'time': f.time, 'contract': f.contract.dict(), 'execution': f.execution.dict(),
            #                'commissionReport': f.commissionReport.dict()} for f in t['fills']]
            # t['log'] = [{'time': l.time, 'status': l.status, 'message': l.message} for l in t['log']]
            save_fill(fill)


    ib.execDetailsEvent += save_trade
    ib.connect(ibhost, ibport, clientId=10, timeout=20)
    print('连接成功')
    fills = ib.fills()
    for f in fills:
        save_fill(f)

    if alive:
        IB.run()
    else:
        print(f'共{len(fills)}条成交记录成功入库')
