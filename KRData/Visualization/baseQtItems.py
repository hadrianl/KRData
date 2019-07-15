#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
@author:Hadrianl
THANKS FOR th github project https://github.com/moonnejs/uiKLine
"""


import numpy as np
import pandas as pd
import pyqtgraph as pg
import datetime as dt

from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *
from PyQt5 import QtGui, QtCore
from pyqtgraph.Point import Point

########################################################################
# 键盘鼠标功能
########################################################################
class KeyWraper(QWidget):
    """键盘鼠标功能支持的元类"""

    # 初始化
    # ----------------------------------------------------------------------
    def __init__(self, parent=None):
        QWidget.__init__(self, parent)
        self.setMouseTracking(True)

    # 重载方法keyPressEvent(self,event),即按键按下事件方法
    # ----------------------------------------------------------------------
    def keyPressEvent(self, event):
        if event.key() == QtCore.Qt.Key_Up:
            self.onUp()
        elif event.key() == QtCore.Qt.Key_Down:
            self.onDown()
        elif event.key() == QtCore.Qt.Key_Left:
            self.onLeft()
        elif event.key() == QtCore.Qt.Key_Right:
            self.onRight()
        elif event.key() == QtCore.Qt.Key_PageUp:
            self.onPre()
        elif event.key() == QtCore.Qt.Key_PageDown:
            self.onNxt()

    # 重载方法mousePressEvent(self,event),即鼠标点击事件方法
    # ----------------------------------------------------------------------
    def mousePressEvent(self, event):
        if event.button() == QtCore.Qt.RightButton:
            self.onRClick(event.pos())
        elif event.button() == QtCore.Qt.LeftButton:
            self.onLClick(event.pos())

    # 重载方法mouseReleaseEvent(self,event),即鼠标点击事件方法
    # ----------------------------------------------------------------------
    def mouseRelease(self, event):
        if event.button() == QtCore.Qt.RightButton:
            self.onRRelease(event.pos())
        elif event.button() == QtCore.Qt.LeftButton:
            self.onLRelease(event.pos())
        self.releaseMouse()

    # 重载方法wheelEvent(self,event),即滚轮事件方法
    # ----------------------------------------------------------------------
    def wheelEvent(self, event):
        return

    # 重载方法paintEvent(self,event),即拖动事件方法
    # ----------------------------------------------------------------------
    def paintEvent(self, event):
        self.onPaint()

    # PgDown键
    # ----------------------------------------------------------------------
    def onNxt(self):
        pass

    # PgUp键
    # ----------------------------------------------------------------------
    def onPre(self):
        pass

    # 向上键和滚轮向上
    # ----------------------------------------------------------------------
    def onUp(self):
        pass

    # 向下键和滚轮向下
    # ----------------------------------------------------------------------
    def onDown(self):
        pass

    # 向左键
    # ----------------------------------------------------------------------
    def onLeft(self):
        pass

    # 向右键
    # ----------------------------------------------------------------------
    def onRight(self):
        pass

    # 鼠标左单击
    # ----------------------------------------------------------------------
    def onLClick(self, pos):
        pass

    # 鼠标右单击
    # ----------------------------------------------------------------------
    def onRClick(self, pos):
        pass

    # 鼠标左释放
    # ----------------------------------------------------------------------
    def onLRelease(self, pos):
        pass

    # 鼠标右释放
    # ----------------------------------------------------------------------
    def onRRelease(self, pos):
        pass

    # 画图
    # ----------------------------------------------------------------------
    def onPaint(self):
        pass


########################################################################
# 选择缩放功能支持
########################################################################
class CustomViewBox(pg.ViewBox):
    # ----------------------------------------------------------------------
    def __init__(self, *args, **kwds):
        pg.ViewBox.__init__(self, *args, **kwds)
        # 拖动放大模式
        # self.setMouseMode(self.RectMode)

    ## 右键自适应
    # ----------------------------------------------------------------------
    def mouseClickEvent(self, ev):
        if ev.button() == QtCore.Qt.RightButton:
            self.autoRange()


########################################################################
# 时间序列，横坐标支持
########################################################################
class MyStringAxis(pg.AxisItem):
    """时间序列横坐标支持"""

    # 初始化
    # ----------------------------------------------------------------------
    def __init__(self, xdict, *args, **kwargs):
        pg.AxisItem.__init__(self, *args, **kwargs)
        self.minVal = 0
        self.maxVal = 0
        self.xdict = xdict
        self.x_values = np.asarray(xdict.keys())
        self.x_strings = xdict.values()
        self.setPen(color=(255, 255, 255, 255), width=0.8)
        self.setStyle(tickFont=QFont("Roman times", 10, QFont.Bold), autoExpandTextSpace=True)

    # 更新坐标映射表
    # ----------------------------------------------------------------------
    def update_xdict(self, xdict):
        self.xdict.update(xdict)
        self.x_values = np.asarray(self.xdict.keys())
        self.x_strings = self.xdict.values()

    # 将原始横坐标转换为时间字符串,第一个坐标包含日期
    # ----------------------------------------------------------------------
    def tickStrings(self, values, scale, spacing):
        strings = []
        for v in values:
            vs = v * scale
            if vs in self.x_values:
                vstr = self.x_strings[np.abs(self.x_values - vs).argmin()]
                vstr = vstr.strftime('%Y-%m-%d %H:%M:%S')
            else:
                vstr = ""
            strings.append(vstr)
        return strings


########################################################################
# K线图形对象
########################################################################
class CandlestickItem(pg.GraphicsObject):
    """K线图形对象"""

    # 初始化
    # ----------------------------------------------------------------------
    def __init__(self, data):
        """初始化"""
        pg.GraphicsObject.__init__(self)
        # 数据格式: [ (time, open, close, low, high),...]
        self.data = data
        # 只重画部分图形，大大提高界面更新速度
        self.rect = None
        self.picture = None
        self.setFlag(self.ItemUsesExtendedStyleOption)
        # 画笔和画刷
        w = 0.4
        self.offset = 0
        self.low = 0
        self.high = 1
        self.picture = QtGui.QPicture()
        self.pictures = []
        self.bPen = pg.mkPen(color=(0, 240, 240, 255), width=w * 2)
        self.bBrush = pg.mkBrush((0, 240, 240, 255))
        self.rPen = pg.mkPen(color=(255, 60, 60, 255), width=w * 2)
        self.rBrush = pg.mkBrush((255, 60, 60, 255))
        self.rBrush.setStyle(Qt.NoBrush)
        self.wPen = pg.mkPen(color='w', width=w * 2)
        self.tickLine = pg.InfiniteLine(angle=0, movable=False, pen=self.wPen)
        self.tickText = pg.InfLineLabel(self.tickLine)
        # 刷新K线
        self.generatePicture(self.data)

        # 画K线


    # ----------------------------------------------------------------------
    def generatePicture(self, data=None, redraw=False):
        """重新生成图形对象"""
        # 重画或者只更新最后一个K线
        if redraw:
            self.pictures = []
        elif self.pictures:
            self.pictures.pop()
        w = 0.4
        bPen = self.bPen
        bBrush = self.bBrush
        rPen = self.rPen
        rBrush = self.rBrush
        self.low, self.high = (np.min(data['low']), np.max(data['high'])) if len(data) > 0 else (0, 1)
        npic = len(self.pictures)
        for (t, open0, close0, low0, high0) in data:
            if t >= npic:
                picture = QtGui.QPicture()
                p = QtGui.QPainter(picture)
                # 下跌蓝色（实心）, 上涨红色（空心）
                pen, brush, pmin, pmax = (bPen, bBrush, close0, open0) \
                    if open0 > close0 else (rPen, rBrush, open0, close0)
                p.setPen(pen)
                p.setBrush(brush)
                self.tickLine.setPen(pen)
                self.tickLine.setPos(close0)
                self.tickText.setText(str(close0), color='w')
                # 画K线方块和上下影线
                if open0 == close0:
                    p.drawLine(QtCore.QPointF(t - w, open0), QtCore.QPointF(t + w, close0))
                else:
                    p.drawRect(QtCore.QRectF(t - w, open0, w * 2, close0 - open0))
                if pmin > low0:
                    p.drawLine(QtCore.QPointF(t, low0), QtCore.QPointF(t, pmin))
                if high0 > pmax:
                    p.drawLine(QtCore.QPointF(t, pmax), QtCore.QPointF(t, high0))
                p.end()
                self.pictures.append(picture)

    # 手动重画
    # ----------------------------------------------------------------------
    def update(self):
        if not self.scene() is None:
            self.scene().update()

    # 自动重画
    # ----------------------------------------------------------------------
    def paint(self, painter, opt, w):
        rect = opt.exposedRect
        xmin, xmax = (max(0, int(rect.left())), min(int(len(self.pictures)), int(rect.right())))
        if self.rect != (rect.left(), rect.right()) or self.picture is None:
            self.rect = (rect.left(), rect.right())
            self.picture = self.createPic(xmin, xmax - 1)
            self.picture.play(painter)
            if self.pictures:
                self.pictures[-1].play(painter)
        elif not self.picture is None:
            # self.picture = self.createPic(xmin, xmax)
            self.picture.play(painter)
            if self.pictures:
                self.pictures[-1].play(painter)
    # 缓存图片
    # ----------------------------------------------------------------------
    def createPic(self, xmin, xmax):
        picture = QPicture()
        p = QPainter(picture)
        [pic.play(p) for pic in self.pictures[xmin:xmax]]
        p.end()
        return picture

    # 定义边界
    # ----------------------------------------------------------------------
    def boundingRect(self):
        return QtCore.QRectF(0, self.low, len(self.pictures), (self.high - self.low))


########################################################################
# 十字光标支持
########################################################################
class Crosshair(QtCore.QObject):
    """
    此类给pg.PlotWidget()添加crossHair功能,PlotWidget实例需要初始化时传入
    """
    signal = QtCore.Signal(type(tuple([])))
    signalInfo = QtCore.Signal(float, float)

    # ----------------------------------------------------------------------
    def __init__(self, parent, master):
        """Constructor"""
        self.__view = parent
        self.master = master
        super(Crosshair, self).__init__()

        self.xAxis = 0
        self.yAxis = 0

        self.datas = None
        self.trade_datas = None

        self.yAxises = [0 for i in range(3)]
        self.leftX = [0 for i in range(3)]
        self.showHLine = [False for i in range(3)]
        self.textPrices = [pg.TextItem('', anchor=(1, 1)) for i in range(3)]
        self.views = [parent.centralWidget.getItem(i + 1, 0) for i in range(3)]
        self.rects = [self.views[i].sceneBoundingRect() for i in range(3)]
        self.vLines = [pg.InfiniteLine(angle=90, movable=False) for i in range(3)]
        self.hLines = [pg.InfiniteLine(angle=0, movable=False) for i in range(3)]

        # mid 在y轴动态跟随最新价显示最新价和最新时间
        self.__textDate = pg.TextItem('date', anchor=(1, 1))
        self.__textInfo = pg.TextItem('lastBarInfo')
        self.__textSig = pg.TextItem('lastSigInfo', anchor=(1, 0))
        self.__textSubSig = pg.TextItem('lastSubSigInfo', anchor=(1, 0))
        self.__textVolume = pg.TextItem('lastBarVolume', anchor=(1, 0))

        self.__textDate.setZValue(2)
        self.__textInfo.setZValue(2)
        self.__textSig.setZValue(2)
        self.__textSubSig.setZValue(2)
        self.__textVolume.setZValue(2)
        self.__textInfo.border = pg.mkPen(color=(230, 255, 0, 255), width=1.2)

        for i in range(3):
            self.textPrices[i].setZValue(2)
            self.vLines[i].setPos(0)
            self.hLines[i].setPos(0)
            self.vLines[i].setZValue(0)
            self.hLines[i].setZValue(0)
            self.views[i].addItem(self.vLines[i])
            self.views[i].addItem(self.hLines[i])
            self.views[i].addItem(self.textPrices[i])

        self.views[0].addItem(self.__textInfo, ignoreBounds=True)
        self.views[0].addItem(self.__textSig, ignoreBounds=True)
        self.views[1].addItem(self.__textVolume, ignoreBounds=True)
        self.views[2].addItem(self.__textDate, ignoreBounds=True)
        self.views[2].addItem(self.__textSubSig, ignoreBounds=True)
        self.proxy = pg.SignalProxy(self.__view.scene().sigMouseMoved, rateLimit=360, slot=self.__mouseMoved)
        # 跨线程刷新界面支持
        self.signal.connect(self.update)
        self.signalInfo.connect(self.plotInfo)

    # ----------------------------------------------------------------------
    def update(self, pos):
        """刷新界面显示"""
        xAxis, yAxis = pos
        xAxis, yAxis = (self.xAxis, self.yAxis) if xAxis is None else (xAxis, yAxis)
        self.moveTo(xAxis, yAxis)

    # ----------------------------------------------------------------------
    def __mouseMoved(self, evt):
        """鼠标移动回调"""
        pos = evt[0]
        self.rects = [self.views[i].sceneBoundingRect() for i in range(3)]
        for i in range(3):
            self.showHLine[i] = False
            if self.rects[i].contains(pos):
                mousePoint = self.views[i].vb.mapSceneToView(pos)
                xAxis = mousePoint.x()
                yAxis = mousePoint.y()
                self.yAxises[i] = yAxis
                self.showHLine[i] = True
                self.moveTo(xAxis, yAxis)

    # ----------------------------------------------------------------------
    def moveTo(self, xAxis, yAxis):
        xAxis, yAxis = (self.xAxis, self.yAxis) if xAxis is None else (int(xAxis), yAxis)
        self.rects = [self.views[i].sceneBoundingRect() for i in range(3)]
        if not xAxis or not yAxis:
            return
        self.xAxis = xAxis
        self.yAxis = yAxis
        self.vhLinesSetXY(xAxis, yAxis)
        self.plotInfo(xAxis, yAxis)
        self.master.volume.update()

    # ----------------------------------------------------------------------
    def vhLinesSetXY(self, xAxis, yAxis):
        """水平和竖线位置设置"""
        for i in range(3):
            self.vLines[i].setPos(xAxis)
            if self.showHLine[i]:
                self.hLines[i].setPos(yAxis if i == 0 else self.yAxises[i])
                self.hLines[i].show()
            else:
                self.hLines[i].hide()

    # ----------------------------------------------------------------------
    def plotInfo(self, xAxis, yAxis):
        """
        被嵌入的plotWidget在需要的时候通过调用此方法显示K线信息
        """
        if self.datas is None:
            return
        try:
            # 获取K线数据
            data = self.datas[xAxis]
            lastdata = self.datas[xAxis - 1]
            tickDatetime = data['datetime']
            openPrice = data['open']
            closePrice = data['close']
            lowPrice = data['low']
            highPrice = data['high']
            volume = int(data['volume'])
            preClosePrice = lastdata['close']
            # tradePrice = abs(self.master.listSig[xAxis])
            trades = self.master.listTrade[self.master.listTrade['time_int']==xAxis]

        except Exception as e:
            return

        if (isinstance(tickDatetime, dt.datetime)):
            datetimeText = dt.datetime.strftime(tickDatetime, '%Y-%m-%d %H:%M:%S')
            dateText = dt.datetime.strftime(tickDatetime, '%Y-%m-%d')
            timeText = dt.datetime.strftime(tickDatetime, '%H:%M:%S')
        elif isinstance(tickDatetime, np.datetime64):
            tickDatetime = tickDatetime.astype(dt.datetime)
            _dt = dt.datetime.fromtimestamp(tickDatetime/1000000000) - dt.timedelta(hours=8)
            datetimeText = _dt.strftime('%Y-%m-%d %H:%M:%S')
            dateText = _dt.strftime('%Y-%m-%d')
            timeText = _dt.strftime('%H:%M:%S')
        else:
            datetimeText = ""
            dateText = ""
            timeText = ""

        # 显示所有的主图技术指标
        html = u'<div style="text-align: right">'
        for sig in self.master.sigData:
            val = self.master.sigData[sig][xAxis]
            col = self.master.sigColor[sig]
            html += u'<span style="color: %s;  font-size: 18px;">&nbsp;&nbsp;%s：%.2f</span>' % (col, sig, val)
        html += u'</div>'
        self.__textSig.setHtml(html)

        # 显示所有的主图技术指标
        html = u'<div style="text-align: right">'
        for sig in self.master.subSigData:
            val = self.master.subSigData[sig][xAxis]
            col = self.master.subSigColor[sig]
            html += u'<span style="color: %s;  font-size: 18px;">&nbsp;&nbsp;%s：%.2f</span>' % (col, sig, val)
        html += u'</div>'
        self.__textSubSig.setHtml(html)

        # 和上一个收盘价比较，决定K线信息的字符颜色
        cOpen = 'red' if openPrice > preClosePrice else 'green'
        cClose = 'red' if closePrice > preClosePrice else 'green'
        cHigh = 'red' if highPrice > preClosePrice else 'green'
        cLow = 'red' if lowPrice > preClosePrice else 'green'

        tradeStr = ''.join(f'<span style="color: {"yellow" if t["direction"] == "short" else "blue"}; font-size: 16px;">'
                           f'{"↓" if t["direction"] == "short" else "↑"}{t["size"]}@{t["price"]}</span><br>' for t in trades)
        self.__textInfo.setHtml(
            u'<div style="text-align: center; background-color:#000">\
                <span style="color: white;  font-size: 16px;">日期</span><br>\
                <span style="color: yellow; font-size: 16px;">%s</span><br>\
                <span style="color: white;  font-size: 16px;">时间</span><br>\
                <span style="color: yellow; font-size: 16px;">%s</span><br>\
                <span style="color: white;  font-size: 16px;">价格</span><br>\
                <span style="color: %s;     font-size: 16px;">(开) %.1f</span><br>\
                <span style="color: %s;     font-size: 16px;">(高) %.1f</span><br>\
                <span style="color: %s;     font-size: 16px;">(低) %.1f</span><br>\
                <span style="color: %s;     font-size: 16px;">(收) %.1f</span><br>\
                <span style="color: white;  font-size: 16px;">(量) %d</span><br>\
                <span style="color: yellow; font-size: 16px;">成交 </span><br>\
                %s\
            </div>' \
            % (dateText, timeText, cOpen, openPrice, cHigh, highPrice, \
               cLow, lowPrice, cClose, closePrice, volume, tradeStr))
        self.__textDate.setHtml(
            '<div style="text-align: center">\
                <span style="color: yellow; font-size: 18px;">%s</span>\
            </div>' \
            % (datetimeText))

        self.__textVolume.setHtml(
            '<div style="text-align: right">\
                <span style="color: white; font-size: 18px;">VOL : %.1f</span>\
            </div>' \
            % (volume))
        # 坐标轴宽度
        rightAxisWidth = self.views[0].getAxis('right').width()
        bottomAxisHeight = self.views[2].getAxis('bottom').height()
        offset = QtCore.QPointF(rightAxisWidth, bottomAxisHeight)

        # 各个顶点
        tl = [self.views[i].vb.mapSceneToView(self.rects[i].topLeft()) for i in range(3)]
        br = [self.views[i].vb.mapSceneToView(self.rects[i].bottomRight() - offset) for i in range(3)]

        # 显示价格
        for i in range(3):
            if self.showHLine[i]:
                self.textPrices[i].setHtml(
                    '<div style="text-align: right">\
                         <span style="color: yellow; font-size: 18px;">\
                           %0.3f\
                         </span>\
                     </div>' \
                    % (yAxis if i == 0 else self.yAxises[i]))
                self.textPrices[i].setPos(br[i].x(), yAxis if i == 0 else self.yAxises[i])
                self.textPrices[i].show()
            else:
                self.textPrices[i].hide()

        # 设置坐标
        self.__textInfo.setPos(tl[0])
        self.__textSig.setPos(br[0].x(), tl[0].y())
        self.__textSubSig.setPos(br[2].x(), tl[2].y())
        self.__textVolume.setPos(br[1].x(), tl[1].y())

        # 修改对称方式防止遮挡
        self.__textDate.anchor = Point((1, 1)) if xAxis > self.master.index else Point((0, 1))
        self.__textDate.setPos(xAxis, br[2].y())
