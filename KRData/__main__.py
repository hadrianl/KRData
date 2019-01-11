#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/2 0002 17:41
# @Author  : Hadrianl 
# @File    : __main__
import click
from .IBTradeRecorder import save_ib_trade
from .CTPDataRecorder import CTPTickRecorder
from .IBDataRecorder import IBTickRecorder
import pandas as pd

@click.group()
def cli():
    click.echo('KRData ToolKit!')

@click.command()
@click.help_option('-h', '--help')
@click.option('--dbhost', default='192.168.2.226', help='数据库host')
@click.option('--dbport', default=27017, help='数据库port')
@click.option('--user',  default='kairuitouzi', help='数据库用户')
@click.option('--password',  prompt=True, type=str, help='数据库密码')
@click.option('--CtpFrontUrl_md', prompt=True, type=str, help='CTP行情前置机')
@click.option('--CtpFrontUrl_td', prompt=True, type=str, help='CTP交易前置机')
@click.option('--CtpUser', prompt=True, type=str, help='CTP帐号')
@click.option('--CtpPwd', prompt=True, type=str, help='CTP密码')
@click.option('--CtpBroker', prompt=True, type=str, help='CTP经纪ID')
# @click.argument('ins_list_file')
def save_ctp_ticker(dbhost, dbport, user, password, ctpfronturl_md, ctpfronturl_td,ctpuser, ctppwd, ctpbroker):
    # click.echo(f'获取合约列表->')
    # ins = pd.read_csv(ins_list_file, header=None)
    # ins_list = ins.iloc[:, 0].tolist()
    # click.echo(f'共{len(ins_list)}个合约:{ins_list}')
    data_recorder = CTPTickRecorder()
    click.echo(f'连接数据库->User:{user}  Host:{dbhost} Port:{dbport}')
    data_recorder.connectDB(user, password, dbhost, dbport)
    click.echo(f'连接CTP->Front:{ctpfronturl_md} User:{ctpuser} Broker:{ctpbroker}')
    data_recorder.connectCTP(ctpfronturl_md, ctpfronturl_td, ctpuser, ctppwd, ctpbroker)
    data_recorder.RecordTicker()

@click.command()
@click.help_option('-h', '--help')
@click.option('--dbhost', default='192.168.2.226', help='数据库host')
@click.option('--dbport', default=27017, help='数据库port')
@click.option('--user',  default='kairuitouzi', help='数据库用户')
@click.option('--password',  prompt=True, help='数据库密码')
@click.option('--ibhost', default='127.0.0.1', help='IB Host')
@click.option('--ibport', default=7496, help='IB Port')
@click.option('--clientId', default=9, help='IB clientId')
@click.argument('ins_list_file')
def save_ib_ticker(dbhost, dbport, user, password, ibhost, ibport, clientid, ins_list_file):
    click.echo(f'获取合约列表->')
    ins = pd.read_csv(ins_list_file, header=None)
    ins_list = ins.iloc[:, 0].tolist()
    click.echo(f'共{len(ins_list)}个合约:{ins_list}')
    data_recorder = IBTickRecorder()
    click.echo(f'连接数据库->User:{user}  Host:{dbhost} Port:{dbport}')
    data_recorder.connectDB(user, password, dbhost, dbport)
    click.echo(f'连接IB->Host:{ibhost} Port:{ibport} clientId:{clientid}')
    data_recorder.connectIB(ibhost, ibport, clientid)
    data_recorder.RecordTicker(ins_list)


cli.add_command(save_ib_trade)
cli.add_command(save_ctp_ticker)
cli.add_command(save_ib_ticker)