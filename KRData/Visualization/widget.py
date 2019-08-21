#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
@author:Hadrianl
THANKS FOR th github project https://github.com/moonnejs/uiKLine
"""

import numpy as np
import pandas as pd
import datetime as dt
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *
from PyQt5 import QtCore
import pyqtgraph as pg
import pyqtgraph.exporters
from functools import partial
from collections import deque
from .baseQtItems import KeyWraper, CandlestickItem, MyStringAxis, Crosshair, CustomViewBox
from ..HKData import HKMarket
from ..IBData import IBMarket, IBTrade
from ..util import _concat_executions, load_json_settings, save_json_settings
from typing import Iterable
import talib
from dateutil import parser

EVENT_BAR_UPDATE = 'eBarUpdate'
DEFAULT_MA_SETTINGS = {'5': 'r', '10': 'b', '30': 'g', '60': 'm'}
DEFAULT_MACD_SETTINGS = {'fastperiod': 12, 'slowperiod': 26, 'signalperiod': 9}
DEFAULT_TRADE_MARK_SETTINGS = {'long': {'angle': 90, 'brush': 'b', 'headLen': 15}, 'short': {'angle': -90, 'brush': 'y', 'headLen': 15}}
DEFAULT_ACCOUNT_SETTINGS = ''
SETTINGS = load_json_settings('visual_settings.json')
MA_SETTINGS = SETTINGS.get('MA', DEFAULT_MA_SETTINGS)
TRADE_MARK_SETTINGS = SETTINGS.get('TradeMark', DEFAULT_TRADE_MARK_SETTINGS)
MACD_SETTINGS = SETTINGS.get('MACD', DEFAULT_MACD_SETTINGS)
ACCOUNT_SETTINGS = SETTINGS.get('ACCOUNT', DEFAULT_ACCOUNT_SETTINGS)
MAX_LEN = 3000

class KLineWidget(KeyWraper):
    """用于显示价格走势图"""

    # 窗口标识
    clsId = 0

    # 保存K线数据的列表和Numpy Array对象
    listBar = []
    listVol = []
    listHigh = []
    listLow = []
    listSig = []
    # listOpenInterest = []
    arrows = []

    # 是否完成了历史数据的读取
    initCompleted = False

    # ----------------------------------------------------------------------
    def __init__(self, data_source='HK', review_mode='live', **kwargs):
        """Constructor"""
        super().__init__()

        self.data_source = data_source
        self.period = kwargs.get('period', '1min')

        if data_source == 'HK':
            self._querier = HKMarket(self.period)
        elif data_source == 'IB':
            self._querier = IBMarket()

        self.review_mode = review_mode

        if review_mode == 'backtest':
            self.setExecutions(kwargs.get('executions', []))
        else:
            self.setExecutions([])


        self.raw_data = None
        # 当前序号
        self.index = None  # 下标
        self.countK = 60  # 显示的Ｋ线范围

        KLineWidget.clsId += 1
        self.windowId = str(KLineWidget.clsId)

        # 缓存数据
        self.datas = []
        self.listBar = []
        self.listVol = []
        self.listHigh = []
        self.listLow = []
        self.listSig = []
        self.listMA = []
        self.listMACD = []
        self.listINCMUL = []
        self.listTrade = []
        self.listHolding = []
        self.dictOrder = {}
        self.arrows = []
        self.tradeArrows = []
        self.tradeLines = []
        # self.tradeTexts = []
        self.orderLines = {}
        self.splitLines = []

        # 所有K线上信号图
        self.allColor = deque(['blue', 'green', 'yellow', 'white'])
        self.sigData = {}
        self.sigColor = {}
        self.sigPlots = {}

        # 所副图上信号图
        self.allSubColor = deque(['blue', 'green', 'yellow', 'white'])
        self.subSigData = {}
        self.subSigColor = {}
        self.subSigPlots = {}

        self.vt_symbol = ''
        self.symbol = ''
        self.exchange = ''
        self.interval = ''
        self.barCount = 300
        self.bar_event_type = ''


        # 初始化完成
        self.initCompleted = False

        # 调用函数
        self.initUi()
        # self.register_event()

    # ----------------------------------------------------------------------
    #  初始化相关
    # ----------------------------------------------------------------------
    def initUi(self):
        """初始化界面"""
        self.setWindowTitle('数据可视化')
        # 主图
        self.pw = pg.PlotWidget()
        # 界面布局
        self.lay_KL = pg.GraphicsLayout(border=(100, 100, 100))
        self.lay_KL.setContentsMargins(10, 10, 10, 10)
        self.lay_KL.setSpacing(0)
        self.lay_KL.setBorder(color=(255, 255, 255, 255), width=0.8)
        self.lay_KL.setZValue(0)
        self.KLtitle = self.lay_KL.addLabel(u'')
        self.pw.setCentralItem(self.lay_KL)
        # 设置横坐标
        xdict = {}
        self.axisTime = MyStringAxis(xdict, orientation='bottom')

        self.indComboBox = QComboBox()
        self.indComboBox.addItems(['MACD', 'INCMUL'])

        # 初始化子图
        self.initplotKline()
        self.initplotVol()
        self.initplotInd()
        # 注册十字光标
        self.crosshair = Crosshair(self.pw, self)
        # 设置界面
        self.vb = QVBoxLayout()

        self.symbol_line = QLineEdit("")
        self.symbol_line.setPlaceholderText('输入产品代码如: HSI1907或者369009605')
        # self.symbol_line.returnPressed.connect(self.subscribe) # todo

        self.interval_combo = QComboBox()
        for inteval in ['1min', '5min', '15min', '30min', '60min', '1day']:
            self.interval_combo.addItem(inteval)

        # self.export_btn = QPushButton('保存')
        # self.export_btn.clicked.connect(self.export_image)

        self.interval_combo.currentTextChanged.connect(self.change_querier_period)
        data_params_layout = QHBoxLayout()
        data_params_layout.addWidget(self.symbol_line)
        data_params_layout.addWidget(self.interval_combo)
        data_params_layout.addWidget(self.indComboBox)
        data_params_layout.addStretch()
        # data_params_layout.addWidget(self.export_btn)
        self.data_params_layout = data_params_layout


        self.datetime_from = QDateTimeEdit()
        self.datetime_to = QDateTimeEdit()
        self.datetime_from.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        self.datetime_to.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        self.datetime_from.setCalendarPopup(True)
        self.datetime_to.setCalendarPopup(True)

        self.trade_links = QCheckBox("Trade Link")
        self.trade_links.stateChanged.connect(self.refreshTradeLinks)

        now = dt.datetime.now()
        self.datetime_from.setDateTime(now - dt.timedelta(days=1))
        self.datetime_to.setDateTime(now)
        timerange_layout = QHBoxLayout()
        timerange_layout.addWidget(self.datetime_from)
        timerange_layout.addWidget(self.datetime_to)
        timerange_layout.addWidget(self.trade_links)
        timerange_layout.addStretch()
        self.account_line = QLineEdit(ACCOUNT_SETTINGS)


        def sourceState(btn):
            if btn.text() == 'HK':
                self._querier = HKMarket(period=self.interval_combo.currentText())
            elif btn.text() == 'IB':
                self._querier = IBMarket()

            self.data_source = btn.text()

        source_layout = QHBoxLayout()
        self.source_HK_btn = QRadioButton('HK')
        self.source_HK_btn.setChecked(True)
        self.source_HK_btn.toggled.connect(lambda: sourceState(self.source_HK_btn))
        self.source_IB_btn = QRadioButton('IB')
        self.source_IB_btn.setToolTip('使用IB查询已过期合约，只能用IB的产品代号如369009605')
        self.source_IB_btn.toggled.connect(lambda: sourceState(self.source_IB_btn))
        source_layout.addWidget(self.source_HK_btn)
        source_layout.addWidget(self.source_IB_btn)
        source_layout.addStretch()

        if self.review_mode == 'backtest':
            self.source_HK_btn.setEnabled(False)
            self.source_HK_btn.setHidden(True)
            self.source_IB_btn.setEnabled(False)
            self.source_IB_btn.setHidden(True)
            self.account_line.setEnabled(False)
            self.account_line.setHidden(True)

        self.query_btn = QPushButton('查询')
        self.query_btn.clicked.connect(self.query_data)

        form = QFormLayout()

        form.addRow(data_params_layout)
        form.addRow('TIME:', timerange_layout)
        if self.review_mode == 'live':
            form.addRow('ACCOUNT:', self.account_line)
            form.addRow('SOURCE:', source_layout)
        elif self.review_mode == 'backtest':
            self.executions_file_btn = QPushButton('读取成交记录')
            self.executions_file_btn.clicked.connect(self.open_executions_file)
            form.addRow(self.executions_file_btn)

        form.addRow(self.query_btn)


        self.vb.addLayout(form)

        self.vb.addWidget(self.pw)
        self.setLayout(self.vb)
        self.resize(1300, 700)
        # 初始化完成
        self.initCompleted = True
        # self.query_data()

        # ----------------------------------------------------------------------
    def closeEvent(self, a0: QCloseEvent) -> None:
        super().closeEvent(a0)
        SETTINGS['ACCOUNT'] = self.account_line.text()
        save_json_settings('visual_settings.json', SETTINGS)

    # def export_image(self):
    #     exporter = pg.exporters.SVGExporter(self.lay_KL)
    #     # print( exporter.parameters())
    #     # exporter.parameters()['width'] = 100
    #     # exporter.parameters()['height'] = 40
    #     exporter.export(f'{self.symbol_line.text()}.svg')

    def open_executions_file(self):
        fname = QFileDialog.getOpenFileName(self, '选择交易文件', './')
        if fname[0]:
            try:
                import pickle
                with open(fname[0], 'rb') as f:
                    executions_list = pickle.load(f)
            finally:
                if isinstance(self.executions, Iterable):
                    self.setExecutions(executions_list)
                    start = self.executions[0]['datetime'] if isinstance(self.executions[0]['datetime'], dt.datetime) else parser.parse(self.executions[0]['datetime'])
                    end = self.executions[-1]['datetime'] if isinstance(self.executions[-1]['datetime'],
                                                                         dt.datetime) else parser.parse(
                        self.executions[-1]['datetime'])
                    self.datetime_from.setDateTime(start - dt.timedelta(minutes=120))
                    self.datetime_to.setDateTime(end + dt.timedelta(minutes=120))
                    self.query_data()

    # def subscribe(self):
    #     if self.symbol != self.symbol_line.text():
    #         self.clearData()
    #         self.symbol = self.symbol_line.text()
    #         print(self.symbol)
    #         self._query_set = self._querier[::self.symbol]

    def query_data(self):
        # self.clearData()
        start = self.datetime_from.dateTime().toPyDateTime()
        end = self.datetime_to.dateTime().toPyDateTime()
        symbol = self.symbol_line.text()

        if self.data_source == 'HK':
            query_set1 = self._querier[120:start:symbol]
            query_set2 = self._querier[start:end:symbol]
            query_set3 = self._querier[end:120:symbol]
            data1 = self._querier.to_df(query_set1)
            data2 = self._querier.to_df(query_set2)
            data3 = self._querier.to_df(query_set3)
            datas = pd.concat([data1, data2, data3])

        elif self.data_source == 'IB':
            contract = self._querier.verifyContract(symbol)
            barType = {'1min': '1 min', '5min': '5 mins', '15min': '15 mins', '30min': '30 mins', '60min': '60 mins', '1day': '1 day'}.get(self.period,'1 min')
            datas = self._querier.get_bars_from_ib(contract, barType=barType, start=start, end=end)


        if self.review_mode == 'backtest':
            executions = self.executions
        elif self.review_mode == 'live':
            acc_id = self.account_line.text()
            if acc_id:
                fills = IBTrade(acc_id)[start:end:symbol]
                fills = fills.order_by('execution.time')

                executions = [{'datetime': f.execution.time + dt.timedelta(hours=8), 'price': f.execution.price,
                      'size': f.execution.shares, 'direction': 'long' if f.execution.side == 'BOT' else 'short'} for f in
                     fills]
            else:
                executions = []

        if executions:
            datas = _concat_executions(datas, executions)

        self.loadData(datas)
        self.refreshAll()

    def setExecutions(self, executions:list):
        self.executions = executions
        if self.executions:
            self.executions.sort(key=lambda t: t['datetime'])

    def change_querier_period(self, period):
        self.period = period
        if self.data_source == 'HK':
            self._querier = HKMarket(period=period)

    def makePI(self, name):
        """生成PlotItem对象"""
        vb = CustomViewBox()
        plotItem = pg.PlotItem(viewBox=vb, name=name, axisItems={'bottom': self.axisTime})
        plotItem.setMenuEnabled(False)
        plotItem.setClipToView(True)
        plotItem.hideAxis('left')
        plotItem.showAxis('right')
        plotItem.setDownsampling(mode='peak')
        plotItem.setRange(xRange=(0, 1), yRange=(0, 1))
        plotItem.getAxis('right').setWidth(60)
        plotItem.getAxis('right').setStyle(tickFont=QFont("Roman times", 10, QFont.Bold))
        plotItem.getAxis('right').setPen(color=(255, 255, 255, 255), width=0.8)
        plotItem.showGrid(True, True)
        plotItem.hideButtons()
        return plotItem

    # ----------------------------------------------------------------------
    def initplotVol(self):
        """初始化成交量子图"""
        self.pwVol = self.makePI('_'.join([self.windowId, 'PlotVOL']))
        self.volume = CandlestickItem(self.listVol)
        self.pwVol.addItem(self.volume)
        self.pwVol.setMaximumHeight(150)
        self.pwVol.setXLink('_'.join([self.windowId, 'PlotInd']))
        self.pwVol.hideAxis('bottom')

        self.lay_KL.nextRow()
        self.lay_KL.addItem(self.pwVol)

    # ----------------------------------------------------------------------
    def initplotKline(self):
        """初始化K线子图"""
        self.pwKL = self.makePI('_'.join([self.windowId, 'PlotKL']))
        self.candle = CandlestickItem(self.listBar)
        self.pwKL.addItem(self.candle)
        self.pwKL.addItem(self.candle.tickLine)
        self.curveMAs = [self.pwKL.plot(pen=c, name=f'ma{p}') for p, c in MA_SETTINGS.items()]
        self.pwKL.setMinimumHeight(350)
        self.pwKL.setXLink('_'.join([self.windowId, 'PlotInd']))
        self.pwKL.hideAxis('bottom')

        self.lay_KL.nextRow()
        self.lay_KL.addItem(self.pwKL)

    # ----------------------------------------------------------------------
    def initplotInd(self):
        """初始化持仓量子图"""
        self.pwInd = self.makePI('_'.join([self.windowId, 'PlotInd']))
        self.curveDif = pg.PlotDataItem(pen='b', name='dif', parent=self.pwInd)
        self.curveDea = pg.PlotDataItem(pen='m', name='dea', parent=self.pwInd)
        self.barMacd = pg.BarGraphItem(x=[0], height=[0], width=0.5, name='macd', parent=self.pwInd)
        self.macdItems = [self.curveDif, self.curveDea, self.barMacd]

        self.curveIncMulP = pg.PlotDataItem(pen='r', name='positiveMul', parent=self.pwInd)
        self.curveIncMulN = pg.PlotDataItem(pen='g', name='negativeMul', parent=self.pwInd)
        self.barInc = pg.BarGraphItem(x=[0], height=[0], width=0.5, name='inc', parent=self.pwInd)
        self.incItems = [self.curveIncMulP, self.curveIncMulN, self.barInc]
        # print(self.pwInd.items)

        def changeInc(inc):
            if inc == 'MACD':
                for item in self.incItems:
                    self.pwInd.removeItem(item)

                print('MACD')
                self.pwInd.addItem(self.curveDif)
                self.pwInd.addItem(self.curveDea)
                self.pwInd.addItem(self.barMacd)
            elif inc == 'INCMUL':
                for item in self.macdItems:
                    self.pwInd.removeItem(item)
                print('INCMUL')
                self.pwInd.addItem(self.curveIncMulP)
                self.pwInd.addItem(self.curveIncMulN)
                self.pwInd.addItem(self.barInc)


        self.indComboBox.currentTextChanged.connect(changeInc)

        changeInc(self.indComboBox.currentText())

        self.lay_KL.nextRow()
        self.lay_KL.addItem(self.pwInd)

    # ----------------------------------------------------------------------
    #  画图相关
    # ----------------------------------------------------------------------
    def plotVol(self, redraw=False, xmin=0, xmax=-1):
        """重画成交量子图"""
        if self.initCompleted:
            self.volume.generatePicture(self.listVol[xmin:xmax], redraw)  # 画成交量子图

    # ----------------------------------------------------------------------
    def plotKline(self, redraw=False, xmin=0, xmax=-1):
        """重画K线子图"""
        if self.initCompleted:
            self.candle.generatePicture(self.listBar[xmin:xmax], redraw)  # 画K线
            for curve in self.curveMAs:
                curve.setData(self.listMA[curve.name()][xmin:xmax])
            self.plotMark()  # 显示开平仓信号位置
            self.plotTradeMark()
            self.plotSplitLines()

    # ----------------------------------------------------------------------
    def plotInd(self, xmin=0, xmax=-1):
        """重画持仓量子图"""
        if self.initCompleted:
            self.curveDif.setData(self.listMACD['dif'][xmin:xmax])
            self.curveDea.setData(self.listMACD['dea'][xmin:xmax])
            self.barMacd.setOpts(x=self.listMACD['time_int'][xmin:xmax], height=self.listMACD['macd'][xmin:xmax],
                                brushes=np.where(self.listMACD['macd'][xmin:xmax]>0, 'r', 'g'))

            self.curveIncMulP.setData(self.listINCMUL['inc_std'][xmin:xmax])
            self.curveIncMulN.setData(-self.listINCMUL['inc_std'][xmin:xmax])
            std_inc_pens = pd.cut(self.listINCMUL['inc_multiple'], [-np.inf, -2, -1, 1, 2, np.inf],
                                  labels=('g', 'y', 'l', 'b', 'r'))
            inc_gt_std = (abs(self.listINCMUL['inc'])/ self.listINCMUL['inc_std']) > 1
            std_inc_brushes = np.where(inc_gt_std, std_inc_pens, None)

            self.barInc.setOpts(x=self.listINCMUL['time_int'][xmin:xmax], height=self.listINCMUL['inc'][xmin:xmax],
                                pens=std_inc_pens, brushes=std_inc_brushes)

            # self.curveOI.setData(np.append(self.listOpenInterest[xmin:xmax], 0), pen='w', name="OpenInterest")

    # ----------------------------------------------------------------------

    def addSig(self, sig, main=True):
        """新增信号图"""
        if main:
            if sig in self.sigPlots:
                self.pwKL.removeItem(self.sigPlots[sig])
            self.sigPlots[sig] = self.pwKL.plot()
            self.sigColor[sig] = self.allColor[0]
            self.allColor.append(self.allColor.popleft())
        else:
            if sig in self.subSigPlots:
                self.pwInd.removeItem(self.subSigPlots[sig])
            self.subSigPlots[sig] = self.pwInd.plot()
            self.subSigColor[sig] = self.allSubColor[0]
            self.allSubColor.append(self.allSubColor.popleft())

    # ----------------------------------------------------------------------
    def showSig(self, datas, main=True, clear=False):
        """刷新信号图"""
        if clear:
            self.clearSig(main)
            if datas and not main:
                sigDatas = np.array(datas.values()[0])
                self.listOpenInterest = sigDatas
                self.datas['openInterest'] = sigDatas
                self.plotOI(0, len(sigDatas))
        if main:
            for sig in datas:
                self.addSig(sig, main)
                self.sigData[sig] = datas[sig]
                self.sigPlots[sig].setData(np.append(datas[sig], 0), pen=self.sigColor[sig][0], name=sig)
        else:
            for sig in datas:
                self.addSig(sig, main)
                self.subSigData[sig] = datas[sig]
                self.subSigPlots[sig].setData(np.append(datas[sig], 0), pen=self.subSigColor[sig][0], name=sig)

    # ----------------------------------------------------------------------
    def plotTradeMark(self):
        """显示交易信号"""
        for arrow in self.tradeArrows:
            self.pwKL.removeItem(arrow)

        for l in self.tradeLines:
            self.pwKL.removeItem(l)

        self.tradeArrows = []
        self.tradeLines = []

        pt = None
        for i, t in enumerate(self.listTrade):
            if t.direction == 'long':
                arrow = pg.ArrowItem(pos=(t.time_int, t.price), angle=TRADE_MARK_SETTINGS['long']['angle'],
                                     brush=TRADE_MARK_SETTINGS['long']['brush'],
                                     headLen=TRADE_MARK_SETTINGS['long']['headLen'] * t.size)
                # text = pg.TextItem(f'{t.size}@{t.price}', color='b', anchor=(t.time_int, t.price), rotateAxis=-45)
                self.pwKL.addItem(arrow)
                self.tradeArrows.append(arrow)
            elif t.direction == 'short':
                arrow = pg.ArrowItem(pos=(t.time_int, t.price), angle=TRADE_MARK_SETTINGS['short']['angle'],
                                     brush=TRADE_MARK_SETTINGS['short']['brush'],
                                     headLen=TRADE_MARK_SETTINGS['short']['headLen'] * t.size)
                # text = pg.TextItem(f'{t.size}@{t.price}', color='y', anchor=(t.time_int, t.price), rotateAxis=45)
                self.pwKL.addItem(arrow)
                self.tradeArrows.append(arrow)

            if pt and pt.direction != t.direction:
                s = min(pt.size, t.size)
                ptv = s * (pt.price if pt.direction == 'short' else -pt.price)
                tv = s * (t.price if t.direction == 'short' else -t.price)
                pen = pg.mkPen('r' if ptv + tv > 0 else 'g', width=1, style=QtCore.Qt.DashLine)
                line = pg.LineSegmentROI([(pt.time_int, pt.price), (t.time_int, t.price)], pen=pen, movable=False)

                self.tradeLines.append(line)
            pt = t

        if self.trade_links.checkState():
            for l in self.tradeLines:
                self.pwKL.addItem(l)

    def refreshTradeLinks(self, b):
        if b:
            for l in self.tradeLines:
                self.pwKL.addItem(l)
        else:
            for l in self.tradeLines:
                self.pwKL.removeItem(l)

    def plotSplitLines(self):
        for sl in self.splitLines:
            self.pwKL.removeItem(sl)
        else:
            self.splitLines.clear()

        pre_x = 0
        for x in self.axisTime.x_values.item():
            pre_t = self.axisTime.xdict[pre_x].time()
            t = self.axisTime.xdict[x].time()
            if  pre_t < dt.time(9, 0) < t or pre_t < dt.time(17, 0) < t or t <  dt.time(17, 0) < pre_t :
                sl = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen(color='w', width=0.8, style=QtCore.Qt.DashDotLine))
                sl.setPos((pre_x + x)/2)
                self.pwKL.addItem(sl)
                self.splitLines.append(sl)

            pre_x = x

    def plotMark(self):
        """显示开平仓信号"""
        # 检查是否有数据
        if len(self.datas) == 0:
            return
        for arrow in self.arrows:
            self.pwKL.removeItem(arrow)
        # 画买卖信号
        for i in range(len(self.listSig)):
            # 无信号
            if self.listSig[i] == 0:
                continue
            # 买信号
            elif self.listSig[i] > 0:
                arrow = pg.ArrowItem(pos=(i, self.datas[i]['low']), angle=90, brush=(255, 0, 0))
            # 卖信号
            elif self.listSig[i] < 0:
                arrow = pg.ArrowItem(pos=(i, self.datas[i]['high']), angle=-90, brush=(0, 255, 0))
            self.pwKL.addItem(arrow)
            self.arrows.append(arrow)

    # ----------------------------------------------------------------------
    def updateAll(self):
        """
        手动更新所有K线图形，K线播放模式下需要
        """
        datas = self.datas
        self.volume.pictrue = None
        self.candle.pictrue = None
        self.volume.update()
        self.candle.update()

        def update(view, low, high):
            vRange = view.viewRange()
            xmin = max(0, int(vRange[0][0]))
            xmax = max(0, int(vRange[0][1]))
            try:
                xmax = min(xmax, len(datas) - 1)
            except:
                xmax = xmax
            if len(datas) > 0 and xmax > xmin:
                ymin = min(datas[xmin:xmax][low])
                ymax = max(datas[xmin:xmax][high])
                view.setRange(yRange=(ymin, ymax))
            else:
                view.setRange(yRange=(0, 1))

        update(self.pwKL.getViewBox(), 'low', 'high')
        update(self.pwVol.getViewBox(), 'volume', 'volume')

    # ----------------------------------------------------------------------
    def plotAll(self, redraw=True, xMin=0, xMax=-1):
        """
        重画所有界面
        redraw ：False=重画最后一根K线; True=重画所有
        xMin,xMax : 数据范围
        """
        xMax = (len(self.datas) - 1)/2 if xMax < 0 else xMax
        # self.countK = xMax-xMin
        # self.index = int((xMax+xMin)/2)
        self.pwInd.setLimits(xMin=xMin, xMax=xMax)
        self.pwKL.setLimits(xMin=xMin, xMax=xMax)
        self.pwVol.setLimits(xMin=xMin, xMax=xMax)
        self.plotKline(redraw, xMin, xMax)  # K线图
        self.plotVol(redraw, xMin, xMax)  # K线副图，成交量
        self.plotInd(0, len(self.datas))  # K线副图，指标
        self.refresh()

    # ----------------------------------------------------------------------
    def refresh(self):
        """
        刷新三个子图的现实范围
        """
        # datas = self.datas
        minutes = int(self.countK / 2)
        xmin = max(0, self.index - minutes)
        try:
            xmax = min(xmin + 2 * minutes, len(self.datas) - 1) if self.datas else xmin + 2 * minutes
        except:
            xmax = xmin + 2 * minutes
        self.pwInd.setRange(xRange=(xmin, xmax))
        self.pwKL.setRange(xRange=(xmin, xmax))
        self.pwVol.setRange(xRange=(xmin, xmax))

    # ----------------------------------------------------------------------
    #  快捷键相关
    # ----------------------------------------------------------------------
    def onNxt(self):
        """跳转到下一个开平仓点"""
        datalen = len(self.listBar)
        if datalen > 0 and not self.index is None:
            if self.index < datalen - 2: self.index += 1
            while self.index < datalen - 2 and self.index not in self.listTrade['time_int']:
                self.index += 1
            self.refresh()
            x = self.index
            y = self.datas[x]['close']
            self.crosshair.signal.emit((x, y))

    # ----------------------------------------------------------------------
    def onPre(self):
        """跳转到上一个开平仓点"""
        datalen = len(self.listBar)
        if datalen > 0 and not self.index is None:
            if self.index > 0: self.index -= 1
            while self.index > 0 and self.index not in self.listTrade['time_int']:
                self.index -= 1
            self.refresh()
            x = self.index
            y = self.datas[x]['close']
            self.crosshair.signal.emit((x, y))

    # ----------------------------------------------------------------------
    def onDown(self):
        """放大显示区间"""
        self.countK = min(len(self.datas), int(self.countK * 1.2) + 1)
        self.refresh()
        if len(self.datas) > 0:
            x = self.index - self.countK / 2 + 2 if int(
                self.crosshair.xAxis) < self.index - self.countK / 2 + 2 else int(self.crosshair.xAxis)
            x = self.index + self.countK / 2 - 2 if x > self.index + self.countK / 2 - 2 else x
            x = len(self.datas) - 1 if x > len(self.datas) - 1 else int(x)
            y = self.datas[x][2]
            self.crosshair.signal.emit((x, y))

    # ----------------------------------------------------------------------
    def onUp(self):
        """缩小显示区间"""
        self.countK = max(3, int(self.countK / 1.2) - 1)
        self.refresh()
        if len(self.datas) > 0:
            x = self.index - self.countK / 2 + 2 if int(
                self.crosshair.xAxis) < self.index - self.countK / 2 + 2 else int(self.crosshair.xAxis)
            x = self.index + self.countK / 2 - 2 if x > self.index + self.countK / 2 - 2 else x
            x = len(self.datas) - 1 if x > len(self.datas) - 1 else int(x)
            y = self.datas[x]['close']
            self.crosshair.signal.emit((x, y))

    # ----------------------------------------------------------------------
    def onLeft(self):
        """向左移动"""
        if len(self.datas) > 0 and int(self.crosshair.xAxis) > 2:
            x = int(self.crosshair.xAxis) - 1
            x = len(self.datas) - 1 if x > len(self.datas) - 1 else int(x)
            y = self.datas[x]['close']
            if x <= self.index - self.countK / 2 + 2 and self.index > 1:
                self.index -= 1
                self.refresh()
            self.crosshair.signal.emit((x, y))

    # ----------------------------------------------------------------------
    def onRight(self):
        """向右移动"""
        if len(self.datas) > 0 and int(self.crosshair.xAxis) < len(self.datas) - 1:
            x = int(self.crosshair.xAxis) + 1
            x = len(self.datas) - 1 if x > len(self.datas) - 1 else int(x)
            y = self.datas[x]['close']
            if x >= self.index + int(self.countK / 2) - 2:
                self.index += 1
                self.refresh()
            self.crosshair.signal.emit((x, y))

    def wheelEvent(self, event):
        """滚轮缩放"""
        angle = event.angleDelta()
        if angle.y() < 0:
            self.onDown()
        elif angle.y() > 0:
            self.onUp()

    # ----------------------------------------------------------------------
    # 界面回调相关
    # ----------------------------------------------------------------------
    def onPaint(self):
        """界面刷新回调"""
        view = self.pwKL.getViewBox()
        vRange = view.viewRange()
        xmin = max(0, int(vRange[0][0]))
        xmax = max(0, int(vRange[0][1]))

        self.index = int((xmin + xmax) / 2) + 1

    # ----------------------------------------------------------------------
    def resignData(self, datas):
        """更新数据，用于Y坐标自适应"""
        self.crosshair.datas = datas
        def viewXRangeChanged(low, high, vb):
            vRange = vb.viewRange()
            xmin = max(0, int(vRange[0][0]))
            xmax = max(0, int(vRange[0][1]))
            xmax = min(xmax, len(datas))
            if len(datas) > 0 and xmax > xmin:
                ymin = min(datas[xmin:xmax][low])
                ymax = max(datas[xmin:xmax][high])
                ymin, ymax = (-1, 1) if ymin == ymax else (ymin, ymax)
                vb.setRange(yRange=(ymin, ymax))
            else:
                vb.setRange(yRange=(0, 1))

        def viewXRangeChanged_Ind(vb):
            vRange = vb.viewRange()
            xmin = max(0, int(vRange[0][0]))
            xmax = max(0, int(vRange[0][1]))
            xmax = min(xmax, len(datas))
            if len(datas) > 0 and xmax > xmin:
                inc = self.indComboBox.currentText()
                if inc == 'MACD':
                    ymin = min(self.listMACD[xmin:xmax]['dif'])
                    ymax = max(self.listMACD[xmin:xmax]['dif'])
                elif inc == 'INCMUL':
                    ymin = min(self.listINCMUL[xmin:xmax]['inc'])
                    ymax = max(self.listINCMUL[xmin:xmax]['inc'])

                ymin, ymax = (-1, 1) if ymin == ymax else (ymin, ymax)
                if not any(np.isnan([ymin, ymax])):
                    vb.setRange(yRange=(ymin, ymax))
            else:
                vb.setRange(yRange=(0, 1))

        view = self.pwKL.getViewBox()
        view.sigXRangeChanged.connect(partial(viewXRangeChanged, 'low', 'high'))

        view = self.pwVol.getViewBox()
        view.sigXRangeChanged.connect(partial(viewXRangeChanged, 'volume', 'volume'))

        view = self.pwInd.getViewBox()
        view.sigXRangeChanged.connect(partial(viewXRangeChanged_Ind))

    # ----------------------------------------------------------------------
    # 数据相关
    # ----------------------------------------------------------------------
    def clearData(self):
        """清空数据"""
        # 清空数据，重新画图
        self.time_index = []
        self.listBar = []
        self.listVol = []
        self.listLow = []
        self.listHigh = []
        self.listMA = []
        self.listMACD = []
        self.listINCMUL = []
        self.listSig = []
        self.sigData = {}
        self.datas = []

        self.listTrade = []
        self.listHolding = []
        self.dictOrder = {}
        self.tradeArrows = []
        self.tradeLines = []
        self.orderLines = {}
        self.splitLines = []

    # ----------------------------------------------------------------------
    def clearSig(self, main=True):
        """清空信号图形"""
        # 清空信号图
        if main:
            for sig in self.sigPlots:
                self.pwKL.removeItem(self.sigPlots[sig])
            self.sigData = {}
            self.sigPlots = {}
        else:
            for sig in self.subSigPlots:
                self.pwInd.removeItem(self.subSigPlots[sig])
            self.subSigData = {}
            self.subSigPlots = {}

    # ----------------------------------------------------------------------
    def updateSig(self, sig):
        """刷新买卖信号"""
        self.listSig = sig
        self.plotMark()

    # ----------------------------------------------------------------------
    def loadData(self, datas: pd.DataFrame,  trades=None, sigs=None):
        """
        载入pandas.DataFrame数据
        datas : 数据格式，cols : datetime, open, close, low, high
        """
        # 设置中心点时间
        # 绑定数据，更新横坐标映射，更新Y轴自适应函数，更新十字光标映射
        for p in MA_SETTINGS.keys():
            datas[f'ma{p}'] = talib.MA(datas['close'].values, int(p))

        datas['dif'], datas['dea'], datas['macd'] = talib.MACDEXT(datas['close'].values, fastperiod=MACD_SETTINGS['fastperiod'], fastmatype=1,
                                                slowperiod=MACD_SETTINGS['slowperiod'], slowmatype=1, signalperiod=MACD_SETTINGS['signalperiod'], signalmatype=1)
        datas['macd'] = datas['macd'] * 2

        datas['inc'] = datas['close'] - datas['open']
        datas['inc_std'] = talib.STDDEV(datas['inc'].values, timeperiod=60)
        datas['inc_multiple'] = (datas['inc'] / datas['inc_std']).fillna(0)

        datas['time_int'] = np.array(range(len(datas)))
        # trades = trades.merge(datas['time_int'], how='left', left_index=True, right_index=True)
        self.datas = datas[['open', 'close', 'low', 'high', 'volume']].to_records()
        self.axisTime.xdict = {}
        xdict = dict(enumerate(datas.index.tolist()))
        self.axisTime.update_xdict(xdict)
        self.resignData(self.datas)
        # 更新画图用到的数据
        self.listBar = datas[['time_int', 'open', 'close', 'low', 'high']].to_records(False)
        self.listHigh = list(datas['high'])
        self.listLow = list(datas['low'])
        self.listMA = datas[[f'ma{p}' for p in MA_SETTINGS.keys()]].to_records(False)
        self.listMACD = datas[['time_int', 'dif', 'dea', 'macd']].to_records(False)
        self.listINCMUL = datas[['time_int', 'inc', 'inc_std', 'inc_multiple']].to_records(False)
        # self.listOpenInterest = list(datas['openInterest'])
        self.listSig = [0] * (len(self.datas) - 1) if sigs is None else sigs
        self.listHolding = []
        if 'trades' in datas.columns:
            trades = []
            holding = []
            pos = 0
            total_value = 0
            for _, row in datas.iterrows():
                if isinstance(row['trades'], Iterable):
                    for t in row['trades']:
                        cur_size = t['size'] if t['direction'] == 'long' else -t['size']
                        pos += cur_size
                        total_value += t['price'] * cur_size
                        trades.append([row['time_int'], t['direction'], t['price'], t['size']])

                holding.append([row['time_int'], pos, total_value])
            else:
                if trades:
                    self.listTrade = pd.DataFrame(trades, columns=['time_int', 'direction', 'price', 'size']).to_records(False)
                    self.listHolding = pd.DataFrame(holding, columns=['time_int', 'pos', 'total_value']).to_records(False)
                else:
                    self.listTrade = []
                    self.listHolding = []
        # 成交量颜色和涨跌同步，K线方向由涨跌决定
        datas0 = pd.DataFrame()
        datas0['open'] = datas.apply(lambda x: 0 if x['close'] >= x['open'] else x['volume'], axis=1)
        datas0['close'] = datas.apply(lambda x: 0 if x['close'] < x['open'] else x['volume'], axis=1)
        datas0['low'] = 0
        datas0['high'] = datas['volume']
        datas0['time_int'] = np.array(range(len(datas.index)))
        self.listVol = datas0[['time_int', 'open', 'close', 'low', 'high']].to_records(False)



    # ----------------------------------------------------------------------
    def refreshAll(self, redraw=True, update=False):
        """
        更新所有界面
        """
        # 调用画图函数
        self.index = len(self.datas)
        self.plotAll(redraw, 0, len(self.datas))
        if not update:
            self.updateAll()
        self.crosshair.signal.emit((None, None))


class ExecutionsMonitor(QWidget):
    """
    For viewing trade result.
    """

    def __init__(
            self, executions: list, parent=None):
        """"""
        super().__init__(parent)
        self.executions = executions
        self.executions_groupby_date = None
        self._selected_executions = []
        self.init_ui()

    def init_ui(self):
        """"""
        self.setWindowTitle(f"执行明细")
        self.resize(1100, 500)

        table = QTableWidget()
        self.table = table
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.verticalHeader().setVisible(False)
        self.table.setSortingEnabled(True)
        self.table.cellDoubleClicked.connect(self.visulize)


        self.read_file_btn = QPushButton('读取成交记录')
        self.read_file_btn.clicked.connect(self.read_trades)

        vbox = QVBoxLayout()
        vbox.addWidget(self.read_file_btn)
        vbox.addWidget(table)

        self.setLayout(vbox)
        self.klineWidget = KLineWidget(data_source='HK', review_mode='backtest')
        self.klineWidget.executions_file_btn.setEnabled(False)

        self.left_btn = QToolButton()
        self.right_btn = QToolButton()
        self.left_btn.setArrowType(Qt.LeftArrow)
        self.right_btn.setArrowType(Qt.RightArrow)
        self._row = 0
        self.klineWidget.data_params_layout.addWidget(self.left_btn)
        self.klineWidget.data_params_layout.addWidget(self.right_btn)
        self.left_btn.clicked.connect(self.pre_visual)
        self.right_btn.clicked.connect(self.nxt_visual)


    def refresh_table(self):
        table = self.table
        table.clearContents()

        table.setColumnCount(13)
        table.setRowCount(len(self.executions_groupby_date))
        table.setHorizontalHeaderLabels(['ID', 'tradeDate', 'symbol', 'long_size', 'long_avg_price', 'short_size', 'short_avg_price',
                                         'net_size', 'net_avg_price', 'net_value',
                                         'holding_pos', 'holding_avg_price', 'holding_value'])
        table.sortByColumn(0, Qt.AscendingOrder)
        holding_pos = 0
        holding_value = 0
        for r, ((t, s), data) in  enumerate(self.executions_groupby_date):
            # print(t, s)
            _id = QTableWidgetItem()
            _id.setData(Qt.DisplayRole, r)
            table.setItem(r, 0, _id)

            tradeDate = QTableWidgetItem()
            tradeDate.setData(Qt.DisplayRole, f'{t}')
            table.setItem(r, 1, tradeDate)

            symbol = QTableWidgetItem()
            symbol.setData(Qt.DisplayRole, f'{s}')
            table.setItem(r, 2, symbol)

            long_size = data[data.direction=='long']['size']
            long_price = data[data.direction=='long']['price']
            short_size = data[data.direction=='short']['size']
            short_price = data[data.direction == 'short']['price']
            long_size_total = long_size.sum()
            long_value_total = (long_price * long_size).sum()
            long_avg_price = long_value_total / long_size_total
            short_size_total = short_size.sum()
            short_value_total = (short_price * short_size).sum()
            short_avg_price = short_value_total / short_size_total
            net_size = long_size_total - short_size_total
            net_value = long_value_total - short_value_total
            net_avg_price = net_value / net_size if net_size != 0 else np.nan
            # print(long_size_total, long_avg_price, short_size_total, short_avg_price)
            holding_pos += net_size
            holding_value = net_value + holding_value
            holding_avg_price = holding_value /holding_pos if holding_pos !=0 else np.nan

            lst = QTableWidgetItem()
            lst.setData(Qt.DisplayRole, int(long_size_total))
            table.setItem(r, 3, lst)

            lap = QTableWidgetItem()
            lap.setData(Qt.DisplayRole, float(long_avg_price))
            table.setItem(r, 4, lap)

            sst = QTableWidgetItem()
            sst.setData(Qt.DisplayRole, int(short_size_total))
            table.setItem(r, 5, sst)

            sap = QTableWidgetItem()
            sap.setData(Qt.DisplayRole, float(short_avg_price))
            table.setItem(r, 6, sap)

            ns = QTableWidgetItem()
            ns.setData(Qt.DisplayRole, float(net_size))
            table.setItem(r, 7, ns)

            nap = QTableWidgetItem()
            nap.setData(Qt.DisplayRole, float(net_avg_price))
            table.setItem(r, 8, nap)

            nv = QTableWidgetItem()
            nv.setData(Qt.DisplayRole, float(net_value))
            table.setItem(r, 9, nv)

            hp = QTableWidgetItem()
            hp.setData(Qt.DisplayRole, float(holding_pos))
            table.setItem(r, 10, hp)

            hvp = QTableWidgetItem()
            hvp.setData(Qt.DisplayRole, float(holding_avg_price))
            table.setItem(r, 11, hvp)

            hv = QTableWidgetItem()
            hv.setData(Qt.DisplayRole, float(holding_value))
            table.setItem(r, 12, hv)

            holding_value = holding_value if holding_pos != 0 else 0

        table.resizeColumnsToContents()

    def read_trades(self):
        fname = QFileDialog.getOpenFileName(self, '选择交易执行文件', './')
        if fname[0]:
            # import pickle
            # with open(fname[0], 'rb') as f:
            #     self.executions = pickle.load(f)
            import pickle
            import os
            _, extension = os.path.splitext(fname[0])
            if extension == 'pkl':
                with open(fname[0], 'rb') as f:
                    executions_list = pickle.load(f)
                    self.executions = pd.DataFrame(executions_list)
            else:
                self.executions = pd.read_excel(fname[0])

            if 'tradeDate' not in self.executions.columns:
                self.executions['tradeDate'] = self.executions.datetime.apply(lambda d: d.date())
            self.executions_groupby_date = self.executions.groupby(['tradeDate', 'symbol'])
            self.refresh_table()

    def visulize(self, row, column):
        if column == 0:
            self._row = row
            r = int(self.table.item(row, 0).text())
            _date = self.table.item(row, 1).text()
            symbol = self.table.item(row, 2).text()
            self._selected_executions = self.executions_groupby_date.get_group((_date, symbol)).to_dict('records')

            if self._selected_executions:
                self.klineWidget.setExecutions(self._selected_executions)
                start = self._selected_executions[0]['datetime']
                end = self._selected_executions[-1]['datetime']
                start = start if isinstance(start, dt.datetime) else parser.parse(start)
                end = end if isinstance(end, dt.datetime) else parser.parse(end)

                self.klineWidget.symbol_line.setText(symbol)

                self.klineWidget.datetime_from.setDateTime(start.replace(hour=0, minute=0, second=0))
                self.klineWidget.datetime_to.setDateTime(end.replace(hour=23, minute=59, second=59))
                self.klineWidget.query_data()
                self.klineWidget.show()
                self.klineWidget.setFocus()

    def pre_visual(self):
        r = max(0, self._row - 1)
        self.visulize(r, 0)

    def nxt_visual(self):
        r = min(self.table.rowCount(), self._row + 1)
        self.visulize(r, 0)

class TradesMonitor(QWidget):
    """
    For viewing trade result.
    """

    def __init__(
            self, trades: list, parent=None):
        """"""
        super().__init__(parent)
        self.trades = trades
        self._selected_executions = []
        self.init_ui()

    def init_ui(self):
        """"""
        self.setWindowTitle(f"交易明细")
        self.resize(1100, 500)

        table = QTableWidget()
        self.table = table
        self.refresh_table()

        self.read_file_btn = QPushButton('读取成交记录')
        self.read_file_btn.clicked.connect(self.read_trades)

        vbox = QVBoxLayout()
        vbox.addWidget(self.read_file_btn)
        vbox.addWidget(table)

        self.setLayout(vbox)
        self.klineWidget = KLineWidget(data_source='HK', review_mode='backtest', parent=self)
        self.klineWidget.executions_file_btn.setEnabled(False)
        self.klineWidget.pwKL.removeItem(self.klineWidget.candle.tickLine)
        self.left_btn = QToolButton()
        self.right_btn = QToolButton()
        self.left_btn.setArrowType(Qt.LeftArrow)
        self.right_btn.setArrowType(Qt.RightArrow)
        self._row = 0
        self.klineWidget.data_params_layout.addWidget(self.left_btn)
        self.klineWidget.data_params_layout.addWidget(self.right_btn)
        self.left_btn.clicked.connect(self.pre_visual)
        self.right_btn.clicked.connect(self.nxt_visual)

    def refresh_table(self):
        table = self.table
        table.clear()
        fields = []
        if not self.trades:
            return

        for k1 in self.trades[0]:
            for k2 in self.trades[0][k1]:
                fields.append(f'{k2}_{k1}')

        # fields = ['datetime_open', 'price_open', 'size_open', 'direction_open', 'datetime_close', 'price_close', 'size_close', 'direction_close']
        table.setColumnCount(len(fields) + 2)
        table.setRowCount(len(self.trades))
        table.setHorizontalHeaderLabels(['ID'] + fields + ['pnl'])
        table.verticalHeader().setVisible(False)

        for r, tradeData in enumerate(self.trades):
            check = QTableWidgetItem()
            check.setData(Qt.DisplayRole, r)
            # check.setCheckState(QtCore.Qt.Unchecked)
            table.setItem(r, 0, check)
            for c, k in enumerate(fields):
                f, oc = k.split('_')
                v = tradeData.get(oc, {}).get(f)
                cell = QTableWidgetItem()
                cell.setFlags(QtCore.Qt.ItemIsEnabled)
                cell.setData(Qt.DisplayRole, v)

                cell.setTextAlignment(QtCore.Qt.AlignCenter)
                table.setItem(r, c + 1, cell)

            _open = tradeData['open']
            open_value = _open['price'] * _open['size'] * (-1 if _open['direction'] == 'long' else 1)
            _close = tradeData['close']
            close_value = _close['price'] * _close['size'] * (-1 if _close['direction'] == 'long' else 1)
            pnl = QTableWidgetItem()
            pnl.setData(Qt.DisplayRole, open_value + close_value)
            table.setItem(r, c + 2, pnl)

        # table.cellChanged.connect(self.visulize)
        table.cellDoubleClicked.connect(self.visulize)
        table.setSortingEnabled(True)

    def read_trades(self):
        fname = QFileDialog.getOpenFileName(self, '选择交易文件', './')
        if fname[0]:
            import pickle
            import os
            _, extension = os.path.splitext(fname[0])
            if extension == 'pkl':
                with open(fname[0], 'rb') as f:
                    self.trades = pickle.load(f)
            else:
                data = pd.read_excel(fname[0])
                self.trades = []
                for _, t in data.iterrows():
                    d = {}
                    for i in t.index:
                        splited = i.split('_')
                        if len(splited) == 2:
                            d.setdefault(splited[1], {})[splited[0]] = t[i]
                        elif len(splited) == 1:
                            d[splited[0]] == t[i]
                    else:
                        self.trades.append(d)

            self.refresh_table()

    def visulize(self, row, column):
        self._row = row
        r = int(self.table.item(row, 0).text())
        self._selected_executions = []
        self._selected_executions.append(self.trades[r]['open'])
        self._selected_executions.append(self.trades[r]['close'])
        self.klineWidget.setExecutions(self._selected_executions)
        if self._selected_executions:
            start = self._selected_executions[0]['datetime']
            end = self._selected_executions[-1]['datetime']
            start = start if isinstance(start, dt.datetime) else parser.parse(start)
            end = start if isinstance(end, dt.datetime) else parser.parse(end)

            symbol = self.trades[r].get('extra', {}).get('symbol', None)
            if symbol:
                self.klineWidget.symbol_line.setText(symbol)
            else:
                self.klineWidget.symbol_line.setText(f'HSI{start.strftime("%y%m")}')

            self.klineWidget.datetime_from.setDateTime(start.replace(hour=0, minute=0, second=0))
            self.klineWidget.datetime_to.setDateTime(end.replace(hour=23, minute=59, second=59))
            self.klineWidget.query_data()
            self.klineWidget.show()

    def pre_visual(self):
        r = max(0, self._row - 1)
        self.visulize(r, 0)

    def nxt_visual(self):
        r = min(self.table.rowCount(), self._row + 1)
        self.visulize(r, 0)