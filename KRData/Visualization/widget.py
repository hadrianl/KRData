#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
@author:Hadrianl
THANKS FOR th github project https://github.com/moonnejs/uiKLine
"""

from vnpy.event import EventEngine, Event
import numpy as np
import pandas as pd
import datetime as dt
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *
from PyQt5 import QtCore
import pyqtgraph as pg
from functools import partial
from collections import deque
from .baseQtItems import KeyWraper, CandlestickItem, MyStringAxis, Crosshair, CustomViewBox
from ..HKData import HKMarket
import talib
from dateutil import parser

EVENT_BAR_UPDATE = 'eBarUpdate'
DEFAULT_MA = [5, 10, 30, 60]
DEFAULT_MA_COLOR = ['r', 'b', 'g', 'y']
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
    signal_bar_update = QtCore.pyqtSignal(Event)
    signal_trade_update = QtCore.pyqtSignal(Event)
    signal_order_update = QtCore.pyqtSignal(Event)

    # ----------------------------------------------------------------------
    def __init__(self, data_source='HK'):
        """Constructor"""
        super().__init__()

        if data_source == 'HK':
            self._querier = HKMarket()


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
        self.listTrade = []
        self.dictOrder = {}
        self.arrows = []
        self.tradeArrows = []
        self.orderLines = {}

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
        # 初始化子图
        self.initplotKline()
        self.initplotVol()
        self.initplotInd()
        # 注册十字光标
        self.crosshair = Crosshair(self.pw, self)
        # 设置界面
        self.vb = QVBoxLayout()

        self.symbol_line = QLineEdit("HSI1906")
        # self.symbol_line.returnPressed.connect(self.subscribe) # todo

        self.interval_combo = QComboBox()
        for inteval in ['1min', '5min', '15min', '30min', '60min', '1day']:
            self.interval_combo.addItem(inteval)

        self.interval_combo.currentTextChanged.connect(self.change_querier)

        self.datetime_from = QDateTimeEdit()
        self.datetime_to = QDateTimeEdit()
        self.datetime_from.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        self.datetime_to.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        self.datetime_from.setCalendarPopup(True)
        self.datetime_to.setCalendarPopup(True)
        self.datetime_from.setDateTime(dt.datetime(2019, 6, 15))
        self.datetime_to.setDateTime(dt.datetime(2019, 6, 20))

        self.query_btn = QPushButton('查询')
        self.query_btn.clicked.connect(self.query_data)

        form = QFormLayout()

        form.addRow("代码", self.symbol_line)
        form.addRow("K线周期", self.interval_combo)
        form.addRow('FROM:', self.datetime_from)
        form.addRow( 'TO:', self.datetime_to)
        form.addRow(self.query_btn)

        self.vb.addLayout(form)

        self.vb.addWidget(self.pw)
        self.setLayout(self.vb)
        self.resize(1300, 700)
        self.signal_bar_update.connect(self.process_bar_event)
        self.signal_trade_update.connect(self.process_trade_event)
        self.signal_order_update.connect(self.process_order_event)
        # 初始化完成
        self.initCompleted = True
        self.query_data()


        # ----------------------------------------------------------------------

    # def subscribe(self):
    #     if self.symbol != self.symbol_line.text():
    #         self.clearData()
    #         self.symbol = self.symbol_line.text()
    #         print(self.symbol)
    #         self._query_set = self._querier[::self.symbol]

    def query_data(self):
        self.loadData(self.datetime_from.dateTime().toPyDateTime(), self.datetime_to.dateTime().toPyDateTime())
        self.refreshAll()

    def change_querier(self, period):
        print(period)
        self._querier = HKMarket(period=period)

    def init_listTrade(self):
        all_trades = self.main_engine.get_all_trades()
        trades = [t for t in all_trades if t.vt_symbol == self.vt_symbol]

        for t in trades:
            # t_time = parser.parse(t.time)
            t_time = t.time
            for i, _time in enumerate(self.axisTime.x_strings):
                timedelta = (t_time - _time).total_seconds()
                if 0 <= timedelta < 60:
                    time_int = i
                    if any(self.listTrade):
                        self.listTrade.resize(len(self.listTrade) + 1, refcheck=0)
                        self.listTrade[-1] = (time_int, t.direction.value, t.price, t.volume)
                    else:
                        self.listTrade = np.rec.array([(time_int, t.direction.value, t.price, t.volume)], \
                                                      names=('time_int', 'direction', 'price', 'volume'))

    def init_dictOrder(self):
        all_orders = self.main_engine.get_all_orders()
        for o in all_orders:
            if o.vt_symbol == self.vt_symbol:
                self.dictOrder[o.vt_orderid] = o

    def process_bar_event(self, event: Event):
        bar = event.data
        self.onBar(bar)
        if len(self.datas) >= self.barCount:

            self.index = len(self.datas)
            vRange = self.pwKL.getViewBox().viewRange()
            xmax = max(0, int(vRange[0][1]))
            if xmax + 10 >= self.index or xmax <= self.countK:
                self.plotAll(False, 0, len(self.datas))
                self.updateAll()
                self.crosshair.signal.emit((None, None))

        elif len(self.datas) >= self.barCount - 1:
            self.init_listTrade()
            self.init_dictOrder()
            self.plotTradeMark()
            self.plotOrderMarkLine()

    def process_trade_event(self, event: Event):
        trade = event.data
        if trade.vt_symbol != self.vt_symbol:
            return

        timedelta = (trade.time - self.datas[-1].datetime).total_seconds()
        time_int = len(self.datas) - (timedelta // 60 + 1)
        if any(self.listTrade):
            self.listTrade.resize(len(self.listTrade) + 1, refcheck=0)
            self.listTrade[-1] = (time_int, trade.direction.value, trade.price, trade.volume)
        else:
            self.listTrade = np.rec.array([(time_int, trade.direction.value, trade.price, trade.volume)], \
                     names=('time_int', 'direction', 'price', 'volume'))

        self.plotTradeMark()
        self.refreshAll(True, False)

    def process_order_event(self, event: Event):
        order = event.data
        if order.vt_symbol != self.vt_symbol:
            return

        self.dictOrder[order.vt_orderid] = order

        self.plotOrderMarkLine()
        self.refreshAll(True, False)

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
        self.curveMAs = [self.pwKL.plot(pen=c, name=f'ma{p}') for p, c in zip(DEFAULT_MA, DEFAULT_MA_COLOR)]
        self.pwKL.setMinimumHeight(350)
        self.pwKL.setXLink('_'.join([self.windowId, 'PlotInd']))
        self.pwKL.hideAxis('bottom')

        self.lay_KL.nextRow()
        self.lay_KL.addItem(self.pwKL)

    # ----------------------------------------------------------------------
    def initplotInd(self):
        """初始化持仓量子图"""
        self.pwInd = self.makePI('_'.join([self.windowId, 'PlotInd']))
        self.curveDif = self.pwInd.plot(pen='w', name='dif')
        self.curveDea = self.pwInd.plot(pen='y', name='dea')
        self.barMacd = pg.BarGraphItem(width=0.5, name='macd')
        self.pwInd.addItem(self.barMacd)
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

    # ----------------------------------------------------------------------
    def plotInd(self, xmin=0, xmax=-1):
        """重画持仓量子图"""
        if self.initCompleted:
            self.curveDif.setData(self.listMACD['dif'][xmin:xmax])
            self.curveDea.setData(self.listMACD['dea'][xmin:xmax])
            self.barMacd.setOpts(x=self.listMACD['time_int'][xmin:xmax], height=self.listMACD['macd'][xmin:xmax],
                                brushes=np.where(self.listMACD['macd'][xmin:xmax]>0, 'r', 'g'))
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

        for t in self.listTrade:
            if t.direction == '多':
                arrow = pg.ArrowItem(pos=(t.time_int, t.price), angle=90, brush=(255, 0, 0))
                self.pwKL.addItem(arrow)
                self.tradeArrows.append(arrow)
            elif t.direction == '空':
                arrow = pg.ArrowItem(pos=(t.time_int, t.price), angle=-90, brush=(0, 255, 0))
                self.pwKL.addItem(arrow)
                self.tradeArrows.append(arrow)

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
        if len(self.listSig) > 0 and not self.index is None:
            datalen = len(self.listSig)
            if self.index < datalen - 2: self.index += 1
            while self.index < datalen - 2 and self.listSig[self.index] == 0:
                self.index += 1
            self.refresh()
            x = self.index
            y = self.datas[x]['close']
            self.crosshair.signal.emit((x, y))

    # ----------------------------------------------------------------------
    def onPre(self):
        """跳转到上一个开平仓点"""
        if len(self.listSig) > 0 and not self.index is None:
            if self.index > 0: self.index -= 1
            while self.index > 0 and self.listSig[self.index] == 0:
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

        def viewXRangeChanged_Ind(low, high, vb):
            vRange = vb.viewRange()
            xmin = max(0, int(vRange[0][0]))
            xmax = max(0, int(vRange[0][1]))
            xmax = min(xmax, len(datas))
            if len(datas) > 0 and xmax > xmin:
                ymin = min(self.listMACD[xmin:xmax][low])
                ymax = max(self.listMACD[xmin:xmax][high])
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
        view.sigXRangeChanged.connect(partial(viewXRangeChanged_Ind, 'dif', 'dif'))

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
        self.listSig = []
        self.sigData = {}
        self.datas = []

        self.listTrade = []
        self.dictOrder = {}
        self.tradeArrows = []
        self.orderLines = {}

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
    def onBar(self, bar):
        """
        新增K线数据,K线播放模式
        """
        # 是否需要更新K线
        newBar = False if len(self.datas) > 0 and bar.datetime == self.datas[-1].datetime else True
        nrecords = len(self.datas) if newBar else len(self.datas) - 1
        # bar.openInterest = np.random.randint(0,
        #                                      3) if bar.openInterest == np.inf or bar.openInterest == -np.inf else bar.openInterest
        openInterest = 0
        recordVol = (nrecords, abs(bar.volume), 0, 0, abs(bar.volume)) if bar.close_price < bar.open_price else (
        nrecords, 0, abs(bar.volume), 0, abs(bar.volume))

        if newBar and any(self.datas):
            self.datas.resize(nrecords + 1, refcheck=0)
            self.listBar.resize(nrecords + 1, refcheck=0)
            self.listVol.resize(nrecords + 1, refcheck=0)
            self.listSig.append(0)
        elif any(self.datas):
            self.listLow.pop()
            self.listHigh.pop()
            # self.listOpenInterest.pop()
            self.listSig.pop()
        if any(self.datas):
            self.datas[-1] = (bar.datetime, bar.open_price, bar.close_price, bar.low_price, bar.high_price, bar.volume, openInterest)
            self.listBar[-1] = (nrecords, bar.open_price, bar.close_price, bar.low_price, bar.high_price)
            self.listVol[-1] = recordVol
            self.listSig[-1] = 0
        else:
            self.datas = np.rec.array(
                [(bar.datetime, bar.open_price, bar.close_price, bar.low_price, bar.high_price, bar.volume, openInterest)], \
                names=('datetime', 'open', 'close', 'low', 'high', 'volume',))
            self.listBar = np.rec.array([(nrecords, bar.open_price, bar.close_price, bar.low_price, bar.high_price)], \
                                        names=('time_int', 'open', 'close', 'low', 'high'))
            self.listVol = np.rec.array([recordVol], names=('time_int', 'open', 'close', 'low', 'high'))
            self.listSig = [0]
            self.resignData(self.datas)

        self.axisTime.update_xdict({nrecords: bar.datetime})
        self.listLow.append(bar.low_price)
        self.listHigh.append(bar.high_price)
        # self.listOpenInterest.append(openInterest)
        self.listSig.append(0)
        self.resignData(self.datas)
        return newBar

    # ----------------------------------------------------------------------
    def loadData(self, start, end, trades=None, sigs=None):
        """
        载入pandas.DataFrame数据
        datas : 数据格式，cols : datetime, open, close, low, high
        """
        # 设置中心点时间
        # 绑定数据，更新横坐标映射，更新Y轴自适应函数，更新十字光标映射

        symbol = self.symbol_line.text()
        query_set = self._querier[start:end:symbol]
        if query_set.count() == 0:
            return
        datas = self._querier.to_df(query_set)

        for p in DEFAULT_MA:
            datas[f'ma{p}'] = talib.MA(datas['close'].values, p)

        datas['dif'], datas['dea'], datas['macd'] = talib.MACDEXT(datas['close'].values, fastperiod=12, fastmatype=1,
                                                slowperiod=26, slowmatype=1, signalperiod=9, signalmatype=1)
        datas['macd'] = datas['macd'] * 2

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
        self.listMA = datas[[f'ma{p}' for p in DEFAULT_MA]].to_records(False)
        self.listMACD = datas[['time_int', 'dif', 'dea', 'macd']].to_records(False)
        # self.listOpenInterest = list(datas['openInterest'])
        self.listSig = [0] * (len(self.datas) - 1) if sigs is None else sigs
        # self.listTrade = trades[['time_int', 'direction', 'price', 'volume']].to_records(False)
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

