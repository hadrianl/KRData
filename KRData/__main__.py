#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/2 0002 17:41
# @Author  : Hadrianl 
# @File    : __main__
import click
from .IBTradeRecorder import save_ib_trade
import pandas as pd
import datetime as dt
import multiprocessing
import time
from . import __version__


def record_data_process(dbhost, dbport, user, password, ctpfronturl_md, ctpfronturl_td, ctpuser, ctppwd, ctpbroker):
    from .CTPDataRecorder import CTPTickRecorder
    data_recorder = CTPTickRecorder()
    click.echo(f'连接数据库->User:{user}  Host:{dbhost} Port:{dbport}')
    data_recorder.connectDB(user, password, dbhost, dbport)
    click.echo(f'连接CTP->Front:{ctpfronturl_md} User:{ctpuser} Broker:{ctpbroker}')
    data_recorder.connectCTP(ctpfronturl_md, ctpfronturl_td, ctpuser, ctppwd, ctpbroker)
    data_recorder.RecordTicker()

@click.group()
def cli():
    click.echo(f'KRData ToolKit!version:{__version__}')

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
            p = multiprocessing.Process(target=record_data_process, args=(dbhost, dbport, user, password, ctpfronturl_md, ctpfronturl_td,ctpuser, ctppwd, ctpbroker))
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
    from .IBDataRecorder import IBTickRecorder
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


@click.command()
@click.option('--review_mode', default='live', type=click.Choice(['live','backtest']))
def visual(review_mode):
    from .Visualization import create_qapp, KLineWidget
    vapp = create_qapp()
    kw = KLineWidget(review_mode=review_mode)
    kw.showMaximized()
    vapp.exec_()

@click.command()
# @click.argument('pkl_file')
def visualTrades():
    from .Visualization import create_qapp, TradesMonitor
    vapp = create_qapp()
    tm = TradesMonitor([])
    tm.show()
    vapp.exec_()

@click.command()
# @click.argument('pkl_file')
def visualExecutions():
    from .Visualization import create_qapp, ExecutionsMonitor
    vapp = create_qapp()
    em = ExecutionsMonitor([])
    em.show()
    vapp.exec_()

@click.command()
@click.help_option('-h', '--help', help='获取盈透数据入库VNPY,start与end不填默认上个交易日的数据')
@click.option('--start', default='', type=str, help='开始日期')
@click.option('--end', default='', type=str, help='结束日期,不填默认到最新数据')
@click.argument('vt_symbol')
def data_record(start, end, vt_symbol):
    from vnpy.trader.database import database_manager
    from vnpy.gateway.ib.ib_gateway import IbGateway
    from vnpy.trader.utility import load_json
    from vnpy.trader.object import HistoryRequest
    from vnpy.trader.constant import Interval, Exchange
    from dateutil import parser
    from vnpy.event.engine import EventEngine
    from vnpy.trader.event import EVENT_LOG

    vt_symbol = vt_symbol
    symbol, exchange = vt_symbol.split('.')

    if not start and not end:
        offset = 0 if dt.datetime.now().time() > dt.time(17, 0) else 1
        start = (dt.datetime.today() - dt.timedelta(days=offset + 1)).replace(hour=17, minute=0, second=0, microsecond=0)
        end = (dt.datetime.today() - dt.timedelta(days=offset)).replace(hour=17, minute=0, second=0, microsecond=0)
    else:
        start = parser.parse(start)
        end = parser.parse(end) if end else end

    ib_settings = load_json('connect_ib.json')
    ib_settings["客户号"] += 4

    recorder_engine = EventEngine()
    def log(event):
        data = event.data
        print(data.level, data.msg)
    recorder_engine.register(EVENT_LOG, log)
    ib = IbGateway(recorder_engine)
    try:
        recorder_engine.start()
        ib.connect(ib_settings)

        if ib.api.client.isConnected():
            req = HistoryRequest(symbol, Exchange(exchange), start, end, Interval.MINUTE)
            ib.write_log(f'发起请求#{vt_symbol}, {start}至{end}')
            his_data = ib.query_history(req)
            ib.write_log(f'获得数据#{vt_symbol}， {his_data[0].datetime}至{his_data[-1].datetime}, 共{len(his_data)}条')
            database_manager.save_bar_data(his_data)
            ib.write_log(f'成功入库')
        else:
            ib.write_log('连接失败！请检查客户号是否被占用或IP是否正确')
    except Exception as e:
        raise e
    finally:
        ib.close()
        recorder_engine.stop()

@click.command()
@click.help_option('-h', '--help', help='获取盈透可用的交易合约')
@click.option('--exchange', default='HKFE', type=str, help='交易所')
@click.option('--secType', default='FUT',type=str, help='标的类型')
@click.option('--expired', default='', type=str, help='到期日')
@click.argument('symbol')
def search_available_contract(exchange, sectype, expired, symbol):
    from vnpy.gateway.ib.ib_gateway import IbGateway
    from vnpy.trader.utility import load_json
    from vnpy.event.engine import EventEngine
    from vnpy.trader.event import EVENT_CONTRACT
    from ibapi.contract import Contract
    import time

    ib_settings = load_json('connect_ib.json')
    ib_settings["客户号"] += 4

    contract_engine = EventEngine()
    def showContract(event):
        contract = event.data
        click.echo(contract, color='red')
    contract_engine.register(EVENT_CONTRACT, showContract)
    ib = IbGateway(contract_engine)
    try:
        contract_engine.start()
        ib.connect(ib_settings)

        if ib.api.client.isConnected():
            contract = Contract()
            contract.symbol = symbol
            contract.exchange = exchange
            contract.secType = sectype
            contract.lastTradeDateOrContractMonth = expired
            ib.api.client.reqContractDetails(1, contract)
            time.sleep(1)
        else:
            ib.write_log('连接失败！请检查客户号是否被占用或IP是否正确')
    except Exception as e:
        raise e
    finally:
        ib.close()
        contract_engine.stop()

cli.add_command(save_ib_trade)
cli.add_command(save_ctp_ticker)
cli.add_command(save_ib_ticker)
cli.add_command(visual)
cli.add_command(visualTrades)
cli.add_command(visualExecutions)
cli.add_command(data_record)
cli.add_command(search_available_contract)