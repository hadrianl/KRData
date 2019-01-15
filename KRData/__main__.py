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
import datetime as dt
import multiprocessing
import time

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
    def record_data_process():
        data_recorder = CTPTickRecorder()
        click.echo(f'连接数据库->User:{user}  Host:{dbhost} Port:{dbport}')
        data_recorder.connectDB(user, password, dbhost, dbport)
        click.echo(f'连接CTP->Front:{ctpfronturl_md} User:{ctpuser} Broker:{ctpbroker}')
        data_recorder.connectCTP(ctpfronturl_md, ctpfronturl_td, ctpuser, ctppwd, ctpbroker)
        data_recorder.RecordTicker()

    DAY_START = dt.time(8, 54)  # 日盘启动和停止时间
    DAY_END = dt.time(15, 21)
    NIGHT_START = dt.time(20, 54)  # 夜盘启动和停止时间
    NIGHT_END = dt.time(2, 36)

    p = None  # 子进程句柄

    while True:
        currentTime = dt.datetime.now().time()
        recording = False

        # 判断当前处于的时间段
        if ((currentTime >= DAY_START and currentTime <= DAY_END) or
                (currentTime >= NIGHT_START) or
                (currentTime <= NIGHT_END)):
            recording = True

        # 过滤周末时间段：周六全天，周五夜盘，周日日盘
        if ((dt.datetime.today().weekday() == 6) or
                (dt.datetime.today().weekday() == 5 and currentTime > NIGHT_END) or
                (dt.datetime.today().weekday() == 0 and currentTime < DAY_START)):
            recording = False

        # 记录时间则需要启动子进程
        if recording and p is None:
            click.echo('启动CTP Tick Recorder')
            p = multiprocessing.Process(target=record_data_process)
            p.start()
            click.echo('CTP Tick Recorder启动成功')

        # 非记录时间则退出子进程
        if not recording and p is not None:
            click.echo('关闭CTP Tick Recorder')
            p.terminate()
            p.join()
            p = None
            click.echo('CTP Tick Recorder关闭成功')

        time.sleep(5)


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