#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
@author:Hadrianl
THANKS FOR th github project https://github.com/moonnejs/uiKLine
"""

import pyqtgraph as pg

from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *
from PyQt5 import QtGui, QtCore

from PyQt5 import QtWidgets
from typing import Dict, List, Type
from .baseObject import *
from contextlib import contextmanager
from collections import defaultdict
from abc import abstractmethod
from dateutil import parser

########################################################################
# 键盘鼠标功能
########################################################################
# class KeyWraper(QWidget):
#     """键盘鼠标功能支持的元类"""
#
#     # 初始化
#     # ----------------------------------------------------------------------
#     def __init__(self, parent=None):
#         QWidget.__init__(self, parent)
#         self.setMouseTracking(True)
#
#     # 重载方法keyPressEvent(self,event),即按键按下事件方法
#     # ----------------------------------------------------------------------
#     def keyPressEvent(self, event):
#         if event.key() == QtCore.Qt.Key_Up:
#             self.onUp()
#         elif event.key() == QtCore.Qt.Key_Down:
#             self.onDown()
#         elif event.key() == QtCore.Qt.Key_Left:
#             self.onLeft()
#         elif event.key() == QtCore.Qt.Key_Right:
#             self.onRight()
#         elif event.key() == QtCore.Qt.Key_PageUp:
#             self.onPre()
#         elif event.key() == QtCore.Qt.Key_PageDown:
#             self.onNxt()
#
#     # 重载方法mousePressEvent(self,event),即鼠标点击事件方法
#     # ----------------------------------------------------------------------
#     def mousePressEvent(self, event):
#         if event.button() == QtCore.Qt.RightButton:
#             self.onRClick(event.pos())
#         elif event.button() == QtCore.Qt.LeftButton:
#             self.onLClick(event.pos())
#
#     # 重载方法mouseReleaseEvent(self,event),即鼠标点击事件方法
#     # ----------------------------------------------------------------------
#     def mouseRelease(self, event):
#         if event.button() == QtCore.Qt.RightButton:
#             self.onRRelease(event.pos())
#         elif event.button() == QtCore.Qt.LeftButton:
#             self.onLRelease(event.pos())
#         self.releaseMouse()
#
#     # 重载方法wheelEvent(self,event),即滚轮事件方法
#     # ----------------------------------------------------------------------
#     def wheelEvent(self, event):
#         return
#
#     # 重载方法paintEvent(self,event),即拖动事件方法
#     # ----------------------------------------------------------------------
#     def paintEvent(self, event):
#         self.onPaint()
#
#     # PgDown键
#     # ----------------------------------------------------------------------
#     def onNxt(self):
#         pass
#
#     # PgUp键
#     # ----------------------------------------------------------------------
#     def onPre(self):
#         pass
#
#     # 向上键和滚轮向上
#     # ----------------------------------------------------------------------
#     def onUp(self):
#         pass
#
#     # 向下键和滚轮向下
#     # ----------------------------------------------------------------------
#     def onDown(self):
#         pass
#
#     # 向左键
#     # ----------------------------------------------------------------------
#     def onLeft(self):
#         pass
#
#     # 向右键
#     # ----------------------------------------------------------------------
#     def onRight(self):
#         pass
#
#     # 鼠标左单击
#     # ----------------------------------------------------------------------
#     def onLClick(self, pos):
#         pass
#
#     # 鼠标右单击
#     # ----------------------------------------------------------------------
#     def onRClick(self, pos):
#         pass
#
#     # 鼠标左释放
#     # ----------------------------------------------------------------------
#     def onLRelease(self, pos):
#         pass
#
#     # 鼠标右释放
#     # ----------------------------------------------------------------------
#     def onRRelease(self, pos):
#         pass
#
#     # 画图
#     # ----------------------------------------------------------------------
#     def onPaint(self):
#         pass
#
#
# ########################################################################
# # 选择缩放功能支持
# ########################################################################
# class CustomViewBox(pg.ViewBox):
#     # ----------------------------------------------------------------------
#     def __init__(self, *args, **kwds):
#         pg.ViewBox.__init__(self, *args, **kwds)
#         # 拖动放大模式
#         # self.setMouseMode(self.RectMode)
#
#     ## 右键自适应
#     # ----------------------------------------------------------------------
#     def mouseClickEvent(self, ev):
#         if ev.button() == QtCore.Qt.RightButton:
#             self.autoRange()
#
#
# ########################################################################
# # 时间序列，横坐标支持
# ########################################################################
# class MyStringAxis(pg.AxisItem):
#     """时间序列横坐标支持"""
#
#     # 初始化
#     # ----------------------------------------------------------------------
#     def __init__(self, xdict, *args, **kwargs):
#         pg.AxisItem.__init__(self, *args, **kwargs)
#         self.minVal = 0
#         self.maxVal = 0
#         self.xdict = xdict
#         self.x_values = np.asarray(xdict.keys())
#         self.x_strings = xdict.values()
#         self.setPen(color=(255, 255, 255, 255), width=0.8)
#         self.setStyle(tickFont=QFont("Roman times", 10, QFont.Bold), autoExpandTextSpace=True)
#
#     # 更新坐标映射表
#     # ----------------------------------------------------------------------
#     def update_xdict(self, xdict):
#         self.xdict.update(xdict)
#         self.x_values = np.asarray(self.xdict.keys())
#         self.x_strings = self.xdict.values()
#
#     # 将原始横坐标转换为时间字符串,第一个坐标包含日期
#     # ----------------------------------------------------------------------
#     def tickStrings(self, values, scale, spacing):
#         strings = []
#         for v in values:
#             vs = v * scale
#             if vs in self.x_values:
#                 vstr = self.x_strings[np.abs(self.x_values - vs).argmin()]
#                 vstr = vstr.strftime('%Y-%m-%d %H:%M:%S')
#             else:
#                 vstr = ""
#             strings.append(vstr)
#         return strings
#
#
# ########################################################################
# # K线图形对象
# ########################################################################
# class CandlestickItem(pg.GraphicsObject):
#     """K线图形对象"""
#
#     # 初始化
#     # ----------------------------------------------------------------------
#     def __init__(self, data):
#         """初始化"""
#         pg.GraphicsObject.__init__(self)
#         # 数据格式: [ (time, open, close, low, high),...]
#         self.data = data
#         # 只重画部分图形，大大提高界面更新速度
#         self.rect = None
#         self.picture = None
#         self.setFlag(self.ItemUsesExtendedStyleOption)
#         # 画笔和画刷
#         w = 0.4
#         self.offset = 0
#         self.low = 0
#         self.high = 1
#         self.picture = QtGui.QPicture()
#         self.pictures = []
#         self.bPen = pg.mkPen(color=(0, 240, 240, 255), width=w * 2)
#         self.bBrush = pg.mkBrush((0, 240, 240, 255))
#         self.rPen = pg.mkPen(color=(255, 60, 60, 255), width=w * 2)
#         self.rBrush = pg.mkBrush((255, 60, 60, 255))
#         self.rBrush.setStyle(Qt.NoBrush)
#         self.wPen = pg.mkPen(color='w', width=w * 2)
#         self.tickLine = pg.InfiniteLine(angle=0, movable=False, pen=self.wPen)
#         self.tickText = pg.InfLineLabel(self.tickLine)
#         # 刷新K线
#         self.generatePicture(self.data)
#
#         # 画K线
#
#
#     # ----------------------------------------------------------------------
#     def generatePicture(self, data=None, redraw=False):
#         """重新生成图形对象"""
#         # 重画或者只更新最后一个K线
#         if redraw:
#             self.pictures = []
#         elif self.pictures:
#             self.pictures.pop()
#         w = 0.4
#         bPen = self.bPen
#         bBrush = self.bBrush
#         rPen = self.rPen
#         rBrush = self.rBrush
#         self.low, self.high = (np.min(data['low']), np.max(data['high'])) if len(data) > 0 else (0, 1)
#         npic = len(self.pictures)
#         for (t, open0, close0, low0, high0) in data:
#             if t >= npic:
#                 picture = QtGui.QPicture()
#                 p = QtGui.QPainter(picture)
#                 # 下跌蓝色（实心）, 上涨红色（空心）
#                 pen, brush, pmin, pmax = (bPen, bBrush, close0, open0) \
#                     if open0 > close0 else (rPen, rBrush, open0, close0)
#                 p.setPen(pen)
#                 p.setBrush(brush)
#                 self.tickLine.setPen(pen)
#                 self.tickLine.setPos(close0)
#                 self.tickText.setText(str(close0), color='w')
#                 # 画K线方块和上下影线
#                 if open0 == close0:
#                     p.drawLine(QtCore.QPointF(t - w, open0), QtCore.QPointF(t + w, close0))
#                 else:
#                     p.drawRect(QtCore.QRectF(t - w, open0, w * 2, close0 - open0))
#                 if pmin > low0:
#                     p.drawLine(QtCore.QPointF(t, low0), QtCore.QPointF(t, pmin))
#                 if high0 > pmax:
#                     p.drawLine(QtCore.QPointF(t, pmax), QtCore.QPointF(t, high0))
#                 p.end()
#                 self.pictures.append(picture)
#
#     # 手动重画
#     # ----------------------------------------------------------------------
#     def update(self):
#         if not self.scene() is None:
#             self.scene().update()
#
#     # 自动重画
#     # ----------------------------------------------------------------------
#     def paint(self, painter, opt, w):
#         rect = opt.exposedRect
#         xmin, xmax = (max(0, int(rect.left())), min(int(len(self.pictures)), int(rect.right())))
#         if self.rect != (rect.left(), rect.right()) or self.picture is None:
#             self.rect = (rect.left(), rect.right())
#             self.picture = self.createPic(xmin, xmax - 1)
#             self.picture.play(painter)
#             if self.pictures:
#                 self.pictures[-1].play(painter)
#         elif not self.picture is None:
#             # self.picture = self.createPic(xmin, xmax)
#             self.picture.play(painter)
#             if self.pictures:
#                 self.pictures[-1].play(painter)
#     # 缓存图片
#     # ----------------------------------------------------------------------
#     def createPic(self, xmin, xmax):
#         picture = QPicture()
#         p = QPainter(picture)
#         [pic.play(p) for pic in self.pictures[xmin:xmax]]
#         p.end()
#         return picture
#
#     # 定义边界
#     # ----------------------------------------------------------------------
#     def boundingRect(self):
#         return QtCore.QRectF(0, self.low, len(self.pictures), (self.high - self.low))
#
#
# ########################################################################
# # 十字光标支持
# ########################################################################
# class Crosshair(QtCore.QObject):
#     """
#     此类给pg.PlotWidget()添加crossHair功能,PlotWidget实例需要初始化时传入
#     """
#     signal = QtCore.Signal(type(tuple([])))
#     signalInfo = QtCore.Signal(float, float)
#
#     # ----------------------------------------------------------------------
#     def __init__(self, parent, master):
#         """Constructor"""
#         self.__view = parent
#         self.master = master
#         super(Crosshair, self).__init__()
#
#         self.xAxis = 0
#         self.yAxis = 0
#
#         self.datas = None
#         self.trade_datas = None
#
#         self.yAxises = [0 for i in range(3)]
#         self.leftX = [0 for i in range(3)]
#         self.showHLine = [False for i in range(3)]
#         self.textPrices = [pg.TextItem('', anchor=(1, 1)) for i in range(3)]
#         self.views = [parent.centralWidget.getItem(i + 1, 0) for i in range(3)]
#         self.rects = [self.views[i].sceneBoundingRect() for i in range(3)]
#         self.vLines = [pg.InfiniteLine(angle=90, movable=False) for i in range(3)]
#         self.hLines = [pg.InfiniteLine(angle=0, movable=False) for i in range(3)]
#
#         # mid 在y轴动态跟随最新价显示最新价和最新时间
#         self.__textDate = pg.TextItem('date', anchor=(1, 1))
#         self.__textInfo = pg.TextItem('lastBarInfo')
#         self.__textSig = pg.TextItem('lastSigInfo', anchor=(1, 0))
#         self.__textSubSig = pg.TextItem('lastSubSigInfo', anchor=(1, 0))
#         self.__textVolume = pg.TextItem('lastBarVolume', anchor=(1, 0))
#         self.__textMAs = pg.TextItem('lastBarMA', anchor=(1, 0))
#
#         self.__textDate.setZValue(2)
#         self.__textInfo.setZValue(2)
#         self.__textSig.setZValue(2)
#         self.__textSubSig.setZValue(2)
#         self.__textVolume.setZValue(2)
#         self.__textMAs.setZValue(2)
#         self.__textInfo.border = pg.mkPen(color=(230, 255, 0, 255), width=1.2)
#
#         for i in range(3):
#             self.textPrices[i].setZValue(2)
#             self.vLines[i].setPos(0)
#             self.hLines[i].setPos(0)
#             self.vLines[i].setZValue(0)
#             self.hLines[i].setZValue(0)
#             self.views[i].addItem(self.vLines[i])
#             self.views[i].addItem(self.hLines[i])
#             self.views[i].addItem(self.textPrices[i])
#
#         self.views[0].addItem(self.__textInfo, ignoreBounds=True)
#         self.views[0].addItem(self.__textSig, ignoreBounds=True)
#         self.views[0].addItem(self.__textMAs, ignoreBounds=True)
#         self.views[1].addItem(self.__textVolume, ignoreBounds=True)
#         self.views[2].addItem(self.__textDate, ignoreBounds=True)
#         self.views[2].addItem(self.__textSubSig, ignoreBounds=True)
#         self.proxy = pg.SignalProxy(self.__view.scene().sigMouseMoved, rateLimit=360, slot=self.__mouseMoved)
#         # 跨线程刷新界面支持
#         self.signal.connect(self.update)
#         self.signalInfo.connect(self.plotInfo)
#
#     # ----------------------------------------------------------------------
#     def update(self, pos):
#         """刷新界面显示"""
#         xAxis, yAxis = pos
#         xAxis, yAxis = (self.xAxis, self.yAxis) if xAxis is None else (xAxis, yAxis)
#         self.moveTo(xAxis, yAxis)
#
#     # ----------------------------------------------------------------------
#     def __mouseMoved(self, evt):
#         """鼠标移动回调"""
#         pos = evt[0]
#         self.rects = [self.views[i].sceneBoundingRect() for i in range(3)]
#         for i in range(3):
#             self.showHLine[i] = False
#             if self.rects[i].contains(pos):
#                 mousePoint = self.views[i].vb.mapSceneToView(pos)
#                 xAxis = mousePoint.x()
#                 yAxis = mousePoint.y()
#                 self.yAxises[i] = yAxis
#                 self.showHLine[i] = True
#                 self.moveTo(xAxis, yAxis)
#
#     # ----------------------------------------------------------------------
#     def moveTo(self, xAxis, yAxis):
#         xAxis, yAxis = (self.xAxis, self.yAxis) if xAxis is None else (int(xAxis), yAxis)
#         self.rects = [self.views[i].sceneBoundingRect() for i in range(3)]
#         if not xAxis or not yAxis:
#             return
#         self.xAxis = xAxis
#         self.yAxis = yAxis
#         self.vhLinesSetXY(xAxis, yAxis)
#         self.plotInfo(xAxis, yAxis)
#         self.master.volume.update()
#
#     # ----------------------------------------------------------------------
#     def vhLinesSetXY(self, xAxis, yAxis):
#         """水平和竖线位置设置"""
#         for i in range(3):
#             self.vLines[i].setPos(xAxis)
#             if self.showHLine[i]:
#                 self.hLines[i].setPos(yAxis if i == 0 else self.yAxises[i])
#                 self.hLines[i].show()
#             else:
#                 self.hLines[i].hide()
#
#     # ----------------------------------------------------------------------
#     def plotInfo(self, xAxis, yAxis):
#         """
#         被嵌入的plotWidget在需要的时候通过调用此方法显示K线信息
#         """
#         if self.datas is None:
#             return
#         try:
#             # 获取K线数据
#             data = self.datas[xAxis]
#             lastdata = self.datas[xAxis - 1]
#             tickDatetime = data['datetime']
#             openPrice = data['open']
#             closePrice = data['close']
#             lowPrice = data['low']
#             highPrice = data['high']
#             volume = int(data['volume'])
#             preClosePrice = lastdata['close']
#             # tradePrice = abs(self.master.listSig[xAxis])
#             trades = self.master.listTrade[self.master.listTrade['time_int']==xAxis]
#             pos = self.master.listHolding[xAxis]
#             mas = self.master.listMA[xAxis]
#
#         except Exception as e:
#             return
#
#         if (isinstance(tickDatetime, dt.datetime)):
#             datetimeText = dt.datetime.strftime(tickDatetime, '%Y-%m-%d %H:%M:%S')
#             dateText = dt.datetime.strftime(tickDatetime, '%Y-%m-%d')
#             timeText = dt.datetime.strftime(tickDatetime, '%H:%M:%S')
#         elif isinstance(tickDatetime, np.datetime64):
#             tickDatetime = tickDatetime.astype(dt.datetime)
#             _dt = dt.datetime.fromtimestamp(tickDatetime/1000000000) - dt.timedelta(hours=8)
#             datetimeText = _dt.strftime('%Y-%m-%d %H:%M:%S')
#             dateText = _dt.strftime('%Y-%m-%d')
#             timeText = _dt.strftime('%H:%M:%S')
#         else:
#             datetimeText = ""
#             dateText = ""
#             timeText = ""
#
#         # 显示所有的主图技术指标
#         html = u'<div style="text-align: right">'
#         for sig in self.master.sigData:
#             val = self.master.sigData[sig][xAxis]
#             col = self.master.sigColor[sig]
#             html += u'<span style="color: %s;  font-size: 18px;">&nbsp;&nbsp;%s：%.2f</span>' % (col, sig, val)
#         html += u'</div>'
#         self.__textSig.setHtml(html)
#
#         # 显示所有的主图技术指标
#         html = u'<div style="text-align: right">'
#         for sig in self.master.subSigData:
#             val = self.master.subSigData[sig][xAxis]
#             col = self.master.subSigColor[sig]
#             html += u'<span style="color: %s;  font-size: 18px;">&nbsp;&nbsp;%s：%.2f</span>' % (col, sig, val)
#         html += u'</div>'
#         self.__textSubSig.setHtml(html)
#
#         # 和上一个收盘价比较，决定K线信息的字符颜色
#         cOpen = 'red' if openPrice > preClosePrice else 'green'
#         cClose = 'red' if closePrice > preClosePrice else 'green'
#         cHigh = 'red' if highPrice > preClosePrice else 'green'
#         cLow = 'red' if lowPrice > preClosePrice else 'green'
#
#         tradeStr = ''.join(f'<span style="color: {"yellow" if t["direction"] == "short" else "blue"}; font-size: 16px;">'
#                            f'{"↓" if t["direction"] == "short" else "↑"}{t["size"]}@{t["price"]}</span><br>' for t in trades)
#
#         posStr = f'{pos["pos"]}@{pos["total_value"]/pos["pos"] if pos["pos"] != 0 else pos["total_value"]: .1f}'
#         self.__textInfo.setHtml(
#             u'<div style="text-align: center; background-color:#000">\
#                 <span style="color: white;  font-size: 16px;">日期</span><br>\
#                 <span style="color: yellow; font-size: 16px;">%s</span><br>\
#                 <span style="color: white;  font-size: 16px;">时间</span><br>\
#                 <span style="color: yellow; font-size: 16px;">%s</span><br>\
#                 <span style="color: white;  font-size: 16px;">价格</span><br>\
#                 <span style="color: %s;     font-size: 16px;">(开) %.1f</span><br>\
#                 <span style="color: %s;     font-size: 16px;">(高) %.1f</span><br>\
#                 <span style="color: %s;     font-size: 16px;">(低) %.1f</span><br>\
#                 <span style="color: %s;     font-size: 16px;">(收) %.1f</span><br>\
#                 <span style="color: white;  font-size: 16px;">(量) %d</span><br>\
#                 <span style="color: yellow; font-size: 16px;">成交 </span><br>\
#                 %s\
#                 <span style="color: yellow; font-size: 16px;">持仓 %s</span><br>\
#             </div>' \
#             % (dateText, timeText, cOpen, openPrice, cHigh, highPrice, \
#                cLow, lowPrice, cClose, closePrice, volume, tradeStr, posStr))
#         self.__textDate.setHtml(
#             '<div style="text-align: center">\
#                 <span style="color: yellow; font-size: 18px;">%s</span>\
#             </div>' \
#             % (datetimeText))
#
#         self.__textVolume.setHtml(
#             '<div style="text-align: right">\
#                 <span style="color: white; font-size: 18px;">VOL : %.1f</span>\
#             </div>' \
#             % (volume))
#
#         maInfo = ''.join(f'<span style="color: {c}; font-size: 18px;">{p} : {v:.2f} </span>' for p, c, v in zip(mas.dtype.names, ['red', 'blue', 'green', 'DeepPink'], mas))
#         self.__textMAs.setHtml(
#             '<div style="text-align: right">\
#                           %s\
#                       </div>' \
#             % (maInfo)
#         )
#
#         # 坐标轴宽度
#         rightAxisWidth = self.views[0].getAxis('right').width()
#         bottomAxisHeight = self.views[2].getAxis('bottom').height()
#         offset = QtCore.QPointF(rightAxisWidth, bottomAxisHeight)
#
#         # 各个顶点
#         tl = [self.views[i].vb.mapSceneToView(self.rects[i].topLeft()) for i in range(3)]
#         br = [self.views[i].vb.mapSceneToView(self.rects[i].bottomRight() - offset) for i in range(3)]
#
#         # 显示价格
#         for i in range(3):
#             if self.showHLine[i]:
#                 self.textPrices[i].setHtml(
#                     '<div style="text-align: right">\
#                          <span style="color: yellow; font-size: 18px;">\
#                            %0.3f\
#                          </span>\
#                      </div>' \
#                     % (yAxis if i == 0 else self.yAxises[i]))
#                 self.textPrices[i].setPos(br[i].x(), yAxis if i == 0 else self.yAxises[i])
#                 self.textPrices[i].show()
#             else:
#                 self.textPrices[i].hide()
#
#         # 设置坐标
#         self.__textInfo.setPos(tl[0])
#         self.__textSig.setPos(br[0].x(), tl[0].y())
#         self.__textSubSig.setPos(br[2].x(), tl[2].y())
#         self.__textVolume.setPos(br[1].x(), tl[1].y())
#         self.__textMAs.setPos(br[0].x(), tl[0].y())
#
#         # 修改对称方式防止遮挡
#         self.__textDate.anchor = Point((1, 1)) if xAxis > self.master.index else Point((0, 1))
#         self.__textDate.setPos(xAxis, br[2].y())


class DatetimeAxis(pg.AxisItem):
    """"""

    def __init__(self, manager: BarManager, *args, **kwargs):
        """"""
        super().__init__(*args, **kwargs)

        self._manager: BarManager = manager

        self.setPen(width=AXIS_WIDTH)
        self.tickFont = NORMAL_FONT

    def tickStrings(self, values: List[int], scale: float, spacing: int):
        """
        Convert original index to datetime string.
        """
        strings = []

        for ix in values:
            dt = self._manager.get_datetime(ix)

            if not dt:
                s = ""
            elif dt.hour:
                s = dt.strftime("%Y-%m-%d\n%H:%M:%S")
            else:
                s = dt.strftime("%Y-%m-%d")

            strings.append(s)

        return strings


class ChartItem(pg.GraphicsObject):
    """"""

    def __init__(self, manager: BarManager):
        """"""
        super().__init__()

        self._manager: BarManager = manager

        self._bar_picutures: Dict[int, QtGui.QPicture] = {}
        self._item_picuture: QtGui.QPicture = None

        self._up_pen: QtGui.QPen = pg.mkPen(
            color=UP_COLOR, width=PEN_WIDTH
        )
        self._up_brush: QtGui.QBrush = pg.mkBrush(color=UP_COLOR)

        self._down_pen: QtGui.QPen = pg.mkPen(
            color=DOWN_COLOR, width=PEN_WIDTH
        )
        self._down_brush: QtGui.QBrush = pg.mkBrush(color=DOWN_COLOR)

        self._rect_area: Tuple[float, float] = None

        # Very important! Only redraw the visible part and improve speed a lot.
        self.setFlag(self.ItemUsesExtendedStyleOption)

    @abstractmethod
    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """
        Draw picture for specific bar.
        """
        pass

    @abstractmethod
    def boundingRect(self) -> QtCore.QRectF:
        """
        Get bounding rectangles for item.
        """
        pass

    @abstractmethod
    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        pass

    @abstractmethod
    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        pass

    def update_history(self, history: List[BarData]) -> BarData:
        """
        Update a list of bar data.
        """
        self._bar_picutures.clear()

        bars = self._manager.get_all_bars()
        for ix, bar in enumerate(bars):
            bar_picture = self._draw_bar_picture(ix, bar)
            self._bar_picutures[ix] = bar_picture

        self.update()

    def update_bar(self, bar: BarData) -> BarData:
        """
        Update single bar data.
        """
        ix = self._manager.get_index(bar.datetime)

        bar_picture = self._draw_bar_picture(ix, bar)
        self._bar_picutures[ix] = bar_picture

        self.update()

    def update(self) -> None:
        """
        Refresh the item.
        """
        if self.scene():
            self.scene().update()

    def paint(
        self,
        painter: QtGui.QPainter,
        opt: QtWidgets.QStyleOptionGraphicsItem,
        w: QtWidgets.QWidget
    ):
        """
        Reimplement the paint method of parent class.

        This function is called by external QGraphicsView.
        """
        rect = opt.exposedRect

        min_ix = int(rect.left())
        max_ix = int(rect.right())
        max_ix = min(max_ix, len(self._bar_picutures))

        rect_area = (min_ix, max_ix)
        if rect_area != self._rect_area or not self._item_picuture:
            self._rect_area = rect_area
            self._draw_item_picture(min_ix, max_ix)

        self._item_picuture.play(painter)

    def _draw_item_picture(self, min_ix: int, max_ix: int) -> None:
        """
        Draw the picture of item in specific range.
        """
        self._item_picuture = QtGui.QPicture()
        painter = QtGui.QPainter(self._item_picuture)

        for n in range(min_ix, max_ix):
            bar_picture = self._bar_picutures[n]
            bar_picture.play(painter)

        painter.end()

    def clear_all(self) -> None:
        """
        Clear all data in the item.
        """
        self._item_picuture = None
        self._bar_picutures.clear()
        self.update()


class CandleItem(ChartItem):
    """"""

    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects
        candle_picture = QtGui.QPicture()
        painter = QtGui.QPainter(candle_picture)

        # Set painter color
        if bar.close_price >= bar.open_price:
            painter.setPen(self._up_pen)
            painter.setBrush(self._up_brush)
        else:
            painter.setPen(self._down_pen)
            painter.setBrush(self._down_brush)

        # Draw candle body
        if bar.open_price == bar.close_price:
            painter.drawLine(
                QtCore.QPointF(ix - BAR_WIDTH, bar.open_price),
                QtCore.QPointF(ix + BAR_WIDTH, bar.open_price),
            )
        else:
            rect = QtCore.QRectF(
                ix - BAR_WIDTH,
                bar.open_price,
                BAR_WIDTH * 2,
                bar.close_price - bar.open_price
            )
            painter.drawRect(rect)

        # Draw candle shadow
        body_bottom = min(bar.open_price, bar.close_price)
        body_top = max(bar.open_price, bar.close_price)

        if bar.low_price < body_bottom:
            painter.drawLine(
                QtCore.QPointF(ix, bar.low_price),
                QtCore.QPointF(ix, body_bottom),
            )

        if bar.high_price > body_top:
            painter.drawLine(
                QtCore.QPointF(ix, bar.high_price),
                QtCore.QPointF(ix, body_top),
            )

        # Finish
        painter.end()
        return candle_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        min_price, max_price = self._manager.get_price_range()
        rect = QtCore.QRectF(
            0,
            min_price,
            len(self._bar_picutures),
            max_price - min_price
        )
        return rect

    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_price, max_price = self._manager.get_price_range(min_ix, max_ix)
        return min_price, max_price

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        bar = self._manager.get_bar(ix)

        if bar:
            words = [
                "Date",
                bar.datetime.strftime("%Y-%m-%d"),
                "",
                "Time",
                bar.datetime.strftime("%H:%M"),
                "",
                "Open",
                str(bar.open_price),
                "",
                "High",
                str(bar.high_price),
                "",
                "Low",
                str(bar.low_price),
                "",
                "Close",
                str(bar.close_price)
            ]
            text = "\n".join(words)
        else:
            text = ""

        return text


class VolumeItem(ChartItem):
    """"""

    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects
        volume_picture = QtGui.QPicture()
        painter = QtGui.QPainter(volume_picture)

        # Set painter color
        if bar.close_price >= bar.open_price:
            painter.setPen(self._up_pen)
            painter.setBrush(self._up_brush)
        else:
            painter.setPen(self._down_pen)
            painter.setBrush(self._down_brush)

        # Draw volume body
        rect = QtCore.QRectF(
            ix - BAR_WIDTH,
            0,
            BAR_WIDTH * 2,
            bar.volume
        )
        painter.drawRect(rect)

        # Finish
        painter.end()
        return volume_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        min_volume, max_volume = self._manager.get_volume_range()
        rect = QtCore.QRectF(
            0,
            min_volume,
            len(self._bar_picutures),
            max_volume - min_volume
        )
        return rect

    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_volume, max_volume = self._manager.get_volume_range(min_ix, max_ix)
        return min_volume, max_volume

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        bar = self._manager.get_bar(ix)

        if bar:
            text = f"Volume {bar.volume}"
        else:
            text = ""

        return text


class MACurveItem(ChartItem):
    name = 'ma'
    plot_name = 'candle'
    MA_PARAMS = [5, 10, 20, 30, 60]
    MA_COLORS = {5: pg.mkPen(color=(255, 255, 255), width=PEN_WIDTH),
                 10: pg.mkPen(color=(255, 255, 0), width=PEN_WIDTH),
                 20: pg.mkPen(color=(218, 112, 214), width=PEN_WIDTH),
                 30: pg.mkPen(color=(0, 255, 0), width=PEN_WIDTH),
                 60: pg.mkPen(color=(64, 224, 208), width=PEN_WIDTH)}
    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)
        # self.periods = [5, 10, 20, 30, 60]
        self.init_setting()
        self._arrayManager = ArrayManager(max(self.MA_PARAMS) + 1)
        self.mas = defaultdict(dict)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()

    def init_setting(self):
        setting = VISUAL_SETTING.get(self.name, {})
        self.MA_PARAMS = setting.get('params', self.MA_PARAMS)
        if 'pen' in setting:
            pen_settings = setting['pen']
            pen_colors = {}
            for p in self.MA_PARAMS:
                pen_colors[p] = pg.mkPen(**pen_settings[str(p)])
            self.MA_COLORS = pen_colors

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects

        if ix <= self.last_ix:
            return self.last_picture

        pre_bar = self._manager.get_bar(ix-1)

        if not pre_bar:
            return self.last_picture

        ma_picture = QtGui.QPicture()
        self._arrayManager.update_bar(pre_bar)
        painter = QtGui.QPainter(ma_picture)

        # Draw volume body
        for p in self.MA_PARAMS:
            if self._arrayManager.close[-(p + 1)] == 0:
                self.mas[p][ix - 1] = np.nan
                continue

            sma=self._arrayManager.ma(p, True)
            pre_ma = sma[-2]
            ma = sma[-1]
            self.mas[p][ix-1] = ma

            sp = QtCore.QPointF(ix-2, pre_ma)
            ep = QtCore.QPointF(ix-1, ma)
            drawPath(painter, sp, ep, self.MA_COLORS[p])

        # Finish
        painter.end()
        self.last_ix = ix
        self.last_picture = ma_picture
        return ma_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        min_price, max_price = self._manager.get_price_range()
        rect = QtCore.QRectF(
            0,
            min_price,
            len(self._bar_picutures),
            max_price - min_price
        )
        return rect

    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_volume, max_volume = self._manager.get_price_range(min_ix, max_ix)
        return min_volume, max_volume

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        text = '\n'.join(f'ma{p}: {v.get(ix, np.nan):.2f}' for p, v in self.mas.items())
        return f"MA \n{text}"

    def clear_all(self) -> None:
        """
        Clear all data in the item.
        """
        super().clear_all()
        self._arrayManager = ArrayManager(max(self.MA_PARAMS) + 1)
        self.mas = defaultdict(dict)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()

class MACDItem(ChartItem):
    name = 'macd'
    plot_name = 'indicator'
    MACD_PARAMS = [12, 26, 9]
    MACD_COLORS = {'diff': pg.mkPen(color=(255, 255, 255), width=PEN_WIDTH),
                 'dea': pg.mkPen(color=(255, 255, 0), width=PEN_WIDTH),
                 'macd': {'up': pg.mkBrush(color=(255, 0, 0)), 'down': pg.mkBrush(color=(0, 255, 50))}}
    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)
        self.init_setting()
        self._arrayManager = ArrayManager(150)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()
        self.macds = defaultdict(dict)
        self.br_max = 0
        self.br_min = 0

    def init_setting(self):
        setting = VISUAL_SETTING.get(self.name, {})
        self.MACD_PARAMS = setting.get('params', self.MACD_PARAMS)
        if 'pen' in setting:
            pen_settings = setting['pen']
            self.MACD_COLORS['diff'] = pg.mkPen(**pen_settings['diff'])
            self.MACD_COLORS['dea'] = pg.mkPen(**pen_settings['dea'])

        if 'brush' in setting:
            brush_settings = setting['brush']
            self.MACD_COLORS['macd'] = {'up': pg.mkBrush(**brush_settings['macd']['up']),
                                        'down': pg.mkBrush(**brush_settings['macd']['down'])}

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects
        if ix <= self.last_ix:
            return self.last_picture

        pre_bar = self._manager.get_bar(ix-1)

        if not pre_bar:
            return self.last_picture

        macd_picture = QtGui.QPicture()
        self._arrayManager.update_bar(pre_bar)
        painter = QtGui.QPainter(macd_picture)

        diff, dea, macd = self._arrayManager.macd(*self.MACD_PARAMS, array=True)
        self.br_max = max(self.br_max, diff[-1], dea[-1], macd[-1])
        self.br_min = min(self.br_min, diff[-1], dea[-1], macd[-1])
        self.macds['diff'][ix-1] = diff[-1]
        self.macds['dea'][ix-1] = dea[-1]
        self.macds['macd'][ix-1] = macd[-1]
        if not (np.isnan(diff[-2]) or np.isnan(dea[-2]) or np.isnan(macd[-1])):
            macd_bar = QtCore.QRectF(ix - 1 - BAR_WIDTH, 0,
                                     BAR_WIDTH * 2, macd[-1])
            painter.setPen(pg.mkPen(color=(255, 255, 255), width=PEN_WIDTH))
            if macd[-1] > 0:
                painter.setBrush(self.MACD_COLORS['macd']['up'])
            else:
                painter.setBrush(self.MACD_COLORS['macd']['down'])
            painter.drawRect(macd_bar)

            diff_sp = QtCore.QPointF(ix - 2, diff[-2])
            diff_ep = QtCore.QPointF(ix - 1, diff[-1])
            drawPath(painter, diff_sp, diff_ep, self.MACD_COLORS['diff'])

            dea_sp = QtCore.QPointF(ix - 2, dea[-2])
            dea_ep = QtCore.QPointF(ix - 1, dea[-1])
            drawPath(painter, dea_sp, dea_ep, self.MACD_COLORS['dea'])

        # Finish
        painter.end()
        self.last_ix = ix
        self.last_picture = macd_picture
        return macd_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        rect = QtCore.QRectF(
            0,
            self.br_min,
            len(self._bar_picutures),
            self.br_max - self.br_min
        )
        return rect


    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_ix = 0 if min_ix is None else min_ix
        max_ix = self.last_ix if max_ix is None else max_ix

        min_v = 0
        max_v = 0

        for i in range(min_ix, max_ix):
            min_v = min(min_v, self.macds['diff'].get(i, min_v), self.macds['dea'].get(i, min_v), self.macds['macd'].get(i, min_v))
            max_v = max(max_v, self.macds['diff'].get(i, max_v), self.macds['dea'].get(i, max_v), self.macds['macd'].get(i, max_v))

        return min_v, max_v

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        return f"MACD{self.MACD_PARAMS}  DIFF:{self.macds['diff'].get(ix, np.nan):.2f} DEA:{self.macds['dea'].get(ix, np.nan):.2f} MACD:{self.macds['macd'].get(ix, np.nan):.2f}"

    def clear_all(self) -> None:
        """
        Clear all data in the item.
        """
        super().clear_all()
        self._arrayManager = ArrayManager(150)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()
        self.macds = defaultdict(dict)
        self.br_max = 0
        self.br_min = 0


class INCItem(ChartItem):
    name = 'inc'
    plot_name = 'indicator'
    INC_PARAMS = [60, 2]
    INC_COLORS = {'up': pg.mkPen(color=(0, 0, 255), width=PEN_WIDTH),
                 'inc': {'up_gte': pg.mkBrush(color=(255, 0, 0)), 'up_lt': pg.mkBrush(color=(160, 32, 240)),
                         'down_gte': pg.mkBrush(color=(0, 255, 0)), 'down_lt': pg.mkBrush(color=(0, 255, 255))},
                 'down': pg.mkPen(color=(255, 255, 0), width=PEN_WIDTH)}
    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)
        self.init_setting()
        self._arrayManager = ArrayManager(150)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()
        self.incs = defaultdict(dict)
        self.br_max = 0
        self.br_min = 0

    def init_setting(self):
        setting = VISUAL_SETTING.get(self.name, {})
        self.INC_PARAMS = setting.get('params', self.INC_PARAMS)
        if 'pen' in setting:
            pen_settings = setting['pen']
            self.INC_COLORS['up'] = pg.mkPen(**pen_settings['up'])
            self.INC_COLORS['down'] = pg.mkPen(**pen_settings['down'])

        if 'brush' in setting:
            brush_settings = setting['brush']
            self.INC_COLORS['inc'] = {'up_gte': pg.mkBrush(**brush_settings['up_gte']),
                                      'up_lt': pg.mkBrush(**brush_settings['up_lt']),
                                      'down_gte': pg.mkBrush(**brush_settings['down_gte']),
                                      'down_lt': pg.mkBrush(**brush_settings['down_lt'])}

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects
        if ix <= self.last_ix:
            return self.last_picture

        pre_bar = self._manager.get_bar(ix-1)

        if not pre_bar:
            return self.last_picture

        inc_picture = QtGui.QPicture()
        self._arrayManager.update_bar(pre_bar)
        painter = QtGui.QPainter(inc_picture)

        inc = self._arrayManager.close - self._arrayManager.open
        std = talib.STDDEV(inc, self.INC_PARAMS[0])
        multiple =  inc / std

        # diff, dea, macd = self._arrayManager.macd(*self.MACD_PARAMS, array=True)
        self.br_max = max(self.br_max, std[-1], inc[-1])
        self.br_min = min(self.br_min, -std[-1], inc[-1])
        self.incs['up'][ix-1] = std[-1]
        self.incs['inc'][ix-1] = inc[-1]
        self.incs['down'][ix-1] = -std[-1]
        self.incs['multiple'][ix-1] = multiple[-1]
        if not (np.isnan(std[-2]*std[-1]*inc[-2]*inc[-1])):
            multiple_bar = QtCore.QRectF(ix - 1 - BAR_WIDTH, 0,
                                         BAR_WIDTH * 2, inc[-1])
            painter.setPen(pg.mkPen(color=(255, 255, 255), width=PEN_WIDTH/2))
            if multiple[-1] >= 0:
                ud = 'up'
            else:
                ud = 'down'
            if abs(multiple[-1]) >= self.INC_PARAMS[1]:
                cp = 'gte'
            else:
                cp = 'lt'
            painter.setBrush(self.INC_COLORS['inc'][f'{ud}_{cp}'])
            painter.drawRect(multiple_bar)

            up_sp = QtCore.QPointF(ix - 2, std[-2])
            up_ep = QtCore.QPointF(ix - 1, std[-1])
            drawPath(painter, up_sp, up_ep, self.INC_COLORS['up'])

            down_sp = QtCore.QPointF(ix - 2, -std[-2])
            down_ep = QtCore.QPointF(ix - 1, -std[-1])
            drawPath(painter, down_sp, down_ep, self.INC_COLORS['down'])

        # Finish
        painter.end()
        self.last_ix = ix
        self.last_picture = inc_picture
        return inc_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        rect = QtCore.QRectF(
            0,
            self.br_min,
            len(self._bar_picutures),
            self.br_max - self.br_min
        )
        return rect


    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_ix = 0 if min_ix is None else min_ix
        max_ix = self.last_ix if max_ix is None else max_ix

        min_v = 0
        max_v = 0

        for i in range(min_ix, max_ix):
            min_v = min(min_v, self.incs['down'].get(i, min_v), self.incs['inc'].get(i, min_v))
            max_v = max(max_v, self.incs['up'].get(i, max_v), self.incs['inc'].get(i, max_v))

        return min_v, max_v

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        return f"INC{self.INC_PARAMS}  UP:{self.incs['up'].get(ix, np.nan):.2f} DOWN:{self.incs['down'].get(ix, np.nan):.2f} INC:{self.incs['inc'].get(ix, np.nan):.2f} MUTIPLE:{self.incs['multiple'].get(ix, np.nan):.2f}"

    def clear_all(self) -> None:
        """
        Clear all data in the item.
        """
        super().clear_all()
        self._arrayManager = ArrayManager(150)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()
        self.incs = defaultdict(dict)
        self.br_max = 0
        self.br_min = 0


class RSICurveItem(ChartItem):
    name = 'rsi'
    plot_name = 'indicator'
    RSI_PARAMS = [6, 12, 24]
    RSI_COLORS = {6: pg.mkPen(color=(255, 255, 255), width=PEN_WIDTH),
                 12: pg.mkPen(color=(255, 255, 0), width=PEN_WIDTH),
                 24: pg.mkPen(color=(218, 112, 214), width=PEN_WIDTH)}
    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)
        # self.periods = [6, 12, 24]
        self.init_setting()
        self._arrayManager = ArrayManager(150)
        self.rsis = defaultdict(dict)
        self.last_ix = 0
        self.br_max = -np.inf
        self.br_min = np.inf
        self.last_picture = QtGui.QPicture()

    def init_setting(self):
        setting = VISUAL_SETTING.get(self.name, {})
        self.RSI_PARAMS = setting.get('params', self.RSI_PARAMS)
        if 'pen' in setting:
            pen_settings = setting['pen']
            for p in self.RSI_PARAMS:
                self.RSI_COLORS[p] = pg.mkPen(**pen_settings[str(p)])

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects

        if ix <= self.last_ix:
            return self.last_picture

        pre_bar = self._manager.get_bar(ix-1)

        if not pre_bar:
            return self.last_picture

        rsi_picture = QtGui.QPicture()
        self._arrayManager.update_bar(pre_bar)
        painter = QtGui.QPainter(rsi_picture)

        # Draw volume body
        for p in self.RSI_PARAMS:
            rsi_=self._arrayManager.rsi(p, True)
            pre_rsi = rsi_[-2]
            rsi = rsi_[-1]
            self.rsis[p][ix-1] = rsi
            if np.isnan(pre_rsi) or np.isnan(rsi):
                continue

            self.br_max = max(self.br_max, rsi_[-1])
            self.br_min = min(self.br_min, rsi_[-1])

            rsi_sp = QtCore.QPointF(ix-2, rsi_[-2])
            rsi_ep = QtCore.QPointF(ix-1, rsi_[-1])
            drawPath(painter, rsi_sp, rsi_ep, self.RSI_COLORS[p])

        # Finish
        painter.end()
        self.last_ix = ix
        self.last_picture = rsi_picture
        return rsi_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        rect = QtCore.QRectF(
            0,
            self.br_min,
            len(self._bar_picutures),
            self.br_max - self.br_min
        )
        return rect

    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_ix = 0 if min_ix is None else min_ix
        max_ix = self.last_ix if max_ix is None else max_ix

        min_v = np.inf
        max_v = -np.inf

        p = self.RSI_PARAMS[0]
        for i in range(min_ix, max_ix):
            min_v = min(min_v, self.rsis[p].get(i, min_v), self.rsis[p].get(i, min_v))
            max_v = max(max_v, self.rsis[p].get(i, max_v), self.rsis[p].get(i, max_v))

        return min_v, max_v

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        text = '\n'.join(f'rsi{p}: {v.get(ix, np.nan):.2f}' for p, v in self.rsis.items())
        return f"RSI \n{text}"

    def clear_all(self) -> None:
        """
        Clear all data in the item.
        """
        super().clear_all()
        self._arrayManager = ArrayManager(150)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()
        self.rsis = defaultdict(dict)
        self.br_max = -np.inf
        self.br_min = np.inf

class PNLCurveItem(ChartItem):
    name = 'pnl'
    plot_name = 'pnl'
    PNL_COLORS = {"up": pg.mkPen(color=(0, 0, 255), width=PEN_WIDTH),
                 "down": pg.mkPen(color=(255, 255, 0), width=PEN_WIDTH)}
    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)
        # self.periods = [6, 12, 24]
        self.ix_pos_map = defaultdict(lambda :(0, 0))
        self.ix_pnl_map = defaultdict(int)
        self.init_setting()
        self.last_ix = 0
        self.br_max = -9999
        self.br_min = 9999
        self.last_picture = QtGui.QPicture()

    def init_setting(self):
        setting = VISUAL_SETTING.get(self.name, {})
        if 'pen' in setting:
            pen_settings = setting['pen']
            for p in self.PNL_COLORS:
                self.PNL_COLORS[p] = pg.mkPen(**pen_settings[str(p)])

    def set_ix_pos_map(self, ix_pos_map):
        self.ix_pos_map = ix_pos_map

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects

        pre_bar = self._manager.get_bar(ix-2)
        bar = self._manager.get_bar(ix-1)

        if not pre_bar:
            return self.last_picture

        pnl_picture = QtGui.QPicture()
        painter = QtGui.QPainter(pnl_picture)

        # Draw volume body
        pre_pos = self.ix_pos_map[ix-2]
        pos = self.ix_pos_map[ix-1]
        if pre_pos[0] == 0:
            pre_pnl = -pre_pos[1]
        else:
            pre_pnl = pre_bar.close_price * pre_pos[0] - pre_pos[1]

        if pos[0] == 0:
            pnl = -pos[1]
        else:
            pnl = bar.close_price * pos[0] -  pos[1]

        self.ix_pnl_map[ix-1] = pnl
        self.br_max = max(self.br_max, pnl)
        self.br_min = min(self.br_min, pnl)

        pnl_sp = QtCore.QPointF(ix-2, pre_pnl)
        pnl_ep = QtCore.QPointF(ix-1, pnl)
        drawPath(painter, pnl_sp, pnl_ep, self.PNL_COLORS['up'])

        # Finish
        painter.end()
        self.last_ix = ix
        self.last_picture = pnl_picture
        return pnl_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        rect = QtCore.QRectF(
            0,
            self.br_min - 10,
            len(self._bar_picutures),
            (self.br_max - self.br_min) + 10
        )

        return rect

    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_ix = 0 if min_ix is None else min_ix
        max_ix = self.last_ix if max_ix is None else max_ix

        min_v = 9999
        max_v = -9999

        for i in range(min_ix, max_ix):
            pnl = self.ix_pnl_map[i]
            min_v = min(min_v, pnl)
            max_v = max(max_v, pnl)

        return min_v - 10 , max_v + 10

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        text = self.ix_pnl_map[ix]
        return f"PNL: \n{text}"

    def clear_all(self) -> None:
        """
        Clear all data in the item.
        """
        super().clear_all()
        self.ix_pos_map = defaultdict(lambda :(0, 0))
        self.ix_pnl_map = defaultdict(int)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()
        self.br_max = -9999
        self.br_min = 9999

class SplitLineItem(ChartItem):
    name = 'split'
    plot_name = 'candle'
    SPLITLINE_PARAMS = ['09:15', '17:15']
    SPLITLINE_COLORS = {'09:15': pg.mkPen(color=(255, 255, 255), width=PEN_WIDTH),
                        '17:15': pg.mkPen(color=(255, 255, 0), width=PEN_WIDTH)}
    def __init__(self, manager: BarManager):
        """"""
        super().__init__(manager)
        # self.periods = [5, 10, 20, 30, 60]
        self.init_setting()
        self.splitLines = defaultdict(dict)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()

    def init_setting(self):
        setting = VISUAL_SETTING.get(self.name, {})
        self.SPLITLINE_PARAMS = setting.get('params', self.SPLITLINE_PARAMS)
        if 'pen' in setting:
            pen_settings = setting['pen']
            pen_colors = {}
            for p in self.SPLITLINE_PARAMS:
                pen_colors[p] = pg.mkPen(**pen_settings[str(p)])
            self.SPLITLINE_COLORS = pen_colors

        for p in self.SPLITLINE_COLORS.values():
            p.setStyle(QtCore.Qt.DashDotDotLine)

    def _draw_bar_picture(self, ix: int, bar: BarData) -> QtGui.QPicture:
        """"""
        # Create objects
        if ix <= self.last_ix:
            return self.last_picture

        splitLine_picture = QtGui.QPicture()
        painter = QtGui.QPainter(splitLine_picture)
        # Draw volume body
        last_bar = self._manager.get_bar(self.last_ix)
        timestr = bar.datetime.time().strftime('%H:%M')
        for t in self.SPLITLINE_PARAMS:
            _time = parser.parse(t).time()
            if _time <= bar.datetime.time() and last_bar.datetime < bar.datetime.replace(hour=_time.hour, minute=_time.minute):
                pen = self.SPLITLINE_COLORS.get(timestr, pg.mkPen(color=(255, 255, 255), width=PEN_WIDTH, style=QtCore.Qt.DashDotDotLine))
                painter.setPen(pen)
                line = QtCore.QLineF(ix-0.5, 0, ix-0.5, 40000)
                self.splitLines[bar.datetime] = line
                painter.drawLine(line)
        # Finish
        painter.end()
        self.last_ix = ix
        self.last_picture = splitLine_picture
        return splitLine_picture

    def boundingRect(self) -> QtCore.QRectF:
        """"""
        rect = QtCore.QRectF(
            0,
            0,
            len(self._bar_picutures),
            40000
        )
        return rect

    def get_y_range(self, min_ix: int = None, max_ix: int = None) -> Tuple[float, float]:
        """
        Get range of y-axis with given x-axis range.

        If min_ix and max_ix not specified, then return range with whole data set.
        """
        min_p, max_p = self._manager.get_price_range(min_ix, max_ix)
        return min_p, max_p

    def get_info_text(self, ix: int) -> str:
        """
        Get information text to show by cursor.
        """
        text = ''
        return text

    def clear_all(self) -> None:
        """
        Clear all data in the item.
        """
        super().clear_all()
        self.splitLines = defaultdict(dict)
        self.last_ix = 0
        self.last_picture = QtGui.QPicture()

def drawPath(painter, sp, ep, color):
    path = QtGui.QPainterPath(sp)
    c1 = QtCore.QPointF((sp.x() + ep.x()) / 2, (sp.y() + ep.y()) / 2)
    c2 = QtCore.QPointF((sp.x() + ep.x()) / 2, (sp.y() + ep.y()) / 2)
    path.cubicTo(c1, c2, ep)
    painter.setPen(color)
    painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
    painter.drawPath(path)

INDICATOR = [MACurveItem, SplitLineItem, MACDItem, INCItem, RSICurveItem]


class ChartWidget(pg.PlotWidget):
    """"""
    MIN_BAR_COUNT = 100

    def __init__(self, parent: QtWidgets.QWidget = None):
        """"""
        super().__init__(parent)

        self._manager: BarManager = BarManager()

        self._plots: Dict[str, pg.PlotItem] = {}
        self._items: Dict[str, ChartItem] = {}
        self._item_plot_map: Dict[ChartItem, pg.PlotItem] = {}

        self._first_plot: pg.PlotItem = None
        self._cursor: ChartCursor = None

        self._right_ix: int = 0                     # Index of most right data
        self._bar_count: int = self.MIN_BAR_COUNT   # Total bar visible in chart

        self._init_ui()

    def _init_ui(self) -> None:
        """"""
        self.setWindowTitle("ChartWidget of vn.py")

        self._layout = pg.GraphicsLayout()
        self._layout.setContentsMargins(10, 10, 10, 10)
        self._layout.setSpacing(0)
        self._layout.setBorder(color=GREY_COLOR, width=0.8)
        self._layout.setZValue(0)
        self.setCentralItem(self._layout)

        self._x_axis = DatetimeAxis(self._manager, orientation='bottom')

    def add_cursor(self) -> None:
        """"""
        if not self._cursor:
            self._cursor = ChartCursor(
                self, self._manager, self._plots, self._item_plot_map)

    def add_plot(
        self,
        plot_name: str,
        minimum_height: int = 80,
        maximum_height: int = None,
        hide_x_axis: bool = False
    ) -> None:
        """
        Add plot area.
        """
        # Create plot object
        plot = pg.PlotItem(axisItems={'bottom': self._x_axis})
        plot.setMenuEnabled(False)
        plot.setClipToView(True)
        plot.hideAxis('left')
        plot.showAxis('right')
        plot.setDownsampling(mode='peak')
        plot.setRange(xRange=(0, 1), yRange=(0, 1))
        plot.hideButtons()
        plot.setMinimumHeight(minimum_height)

        if maximum_height:
            plot.setMaximumHeight(maximum_height)

        if hide_x_axis:
            plot.hideAxis("bottom")

        if not self._first_plot:
            self._first_plot = plot

        # Connect view change signal to update y range function
        view = plot.getViewBox()
        view.sigXRangeChanged.connect(self._update_y_range)
        view.setMouseEnabled(x=True, y=False)

        # Set right axis
        right_axis = plot.getAxis('right')
        right_axis.setWidth(60)
        right_axis.tickFont = NORMAL_FONT

        # Connect x-axis link
        if self._plots:
            first_plot = list(self._plots.values())[0]
            plot.setXLink(first_plot)

        # Store plot object in dict
        self._plots[plot_name] = plot

        # Add plot onto the layout
        self._layout.nextRow()
        self._layout.addItem(plot)

    def add_item(
        self,
        item_class: Type[ChartItem],
        item_name: str,
        plot_name: str
    ):
        """
        Add chart item.
        """
        item = item_class(self._manager)
        self._items[item_name] = item

        plot = self._plots.get(plot_name)
        plot.addItem(item)

        self._item_plot_map[item] = plot

    def get_plot(self, plot_name: str) -> pg.PlotItem:
        """
        Get specific plot with its name.
        """
        return self._plots.get(plot_name, None)

    def get_all_plots(self) -> List[pg.PlotItem]:
        """
        Get all plot objects.
        """
        return self._plots.values()

    def clear_all(self) -> None:
        """
        Clear all data.
        """
        self._manager.clear_all()

        for item in self._items.values():
            item.clear_all()

        if self._cursor:
            self._cursor.clear_all()

    def update_history(self, history: List[BarData]) -> None:
        """
        Update a list of bar data.
        """
        self._manager.update_history(history)

        for item in self._items.values():
            item.update_history(history)

        self._update_plot_limits()

        self.move_to_right()

    def update_bar(self, bar: BarData) -> None:
        """
        Update single bar data.
        """
        self._manager.update_bar(bar)

        for item in self._items.values():
            item.update_bar(bar)

        self._update_plot_limits()

        if self._right_ix >= (self._manager.get_count() - self._bar_count / 2):
            self.move_to_right()

    def _update_plot_limits(self) -> None:
        """
        Update the limit of plots.
        """
        for item, plot in self._item_plot_map.items():
            min_value, max_value = item.get_y_range()

            plot.setLimits(
                xMin=-1,
                xMax=self._manager.get_count(),
                yMin=min_value,
                yMax=max_value
            )

    def _update_x_range(self) -> None:
        """
        Update the x-axis range of plots.
        """
        max_ix = self._right_ix
        min_ix = self._right_ix - self._bar_count

        for plot in self._plots.values():
            plot.setRange(xRange=(min_ix, max_ix), padding=0)

    def _update_y_range(self) -> None:
        """
        Update the y-axis range of plots.
        """
        view = self._first_plot.getViewBox()
        view_range = view.viewRange()

        min_ix = max(0, int(view_range[0][0]))
        max_ix = min(self._manager.get_count(), int(view_range[0][1]))

        # Update limit for y-axis
        for item, plot in self._item_plot_map.items():
            y_range = item.get_y_range(min_ix, max_ix)
            plot.setRange(yRange=y_range)

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:
        """
        Reimplement this method of parent to update current max_ix value.
        """
        view = self._first_plot.getViewBox()
        view_range = view.viewRange()
        self._right_ix = max(0, view_range[0][1])

        super().paintEvent(event)

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        """
        Reimplement this method of parent to move chart horizontally and zoom in/out.
        """
        if event.key() == QtCore.Qt.Key_Left:
            self._on_key_left()
        elif event.key() == QtCore.Qt.Key_Right:
            self._on_key_right()
        elif event.key() == QtCore.Qt.Key_Up:
            self._on_key_up()
        elif event.key() == QtCore.Qt.Key_Down:
            self._on_key_down()

    def wheelEvent(self, event: QtGui.QWheelEvent) -> None:
        """
        Reimplement this method of parent to zoom in/out.
        """
        delta = event.angleDelta()

        if delta.y() > 0:
            self._on_key_up()
        elif delta.y() < 0:
            self._on_key_down()

    def _on_key_left(self) -> None:
        """
        Move chart to left.
        """
        self._right_ix -= 1
        self._right_ix = max(self._right_ix, self._bar_count)

        self._update_x_range()
        self._cursor.move_left()
        self._cursor.update_info()

    def _on_key_right(self) -> None:
        """
        Move chart to right.
        """
        self._right_ix += 1
        self._right_ix = min(self._right_ix, self._manager.get_count())

        self._update_x_range()
        self._cursor.move_right()
        self._cursor.update_info()

    def _on_key_down(self) -> None:
        """
        Zoom out the chart.
        """
        self._bar_count *= 1.2
        self._bar_count = min(int(self._bar_count), self._manager.get_count())

        self._update_x_range()
        self._cursor.update_info()

    def _on_key_up(self) -> None:
        """
        Zoom in the chart.
        """
        self._bar_count /= 1.2
        self._bar_count = max(int(self._bar_count), self.MIN_BAR_COUNT)

        self._update_x_range()
        self._cursor.update_info()

    def move_to_right(self) -> None:
        """
        Move chart to the most right.
        """
        self._right_ix = self._manager.get_count()
        self._update_x_range()
        self._cursor.update_info()


class ChartCursor(QtCore.QObject):
    """"""

    def __init__(
        self,
        widget: ChartWidget,
        manager: BarManager,
        plots: Dict[str, pg.GraphicsObject],
        item_plot_map: Dict[ChartItem, pg.GraphicsObject]
    ):
        """"""
        super().__init__()

        self._widget: ChartWidget = widget
        self._manager: BarManager = manager
        self._plots: Dict[str, pg.GraphicsObject] = plots
        self._item_plot_map: Dict[ChartItem, pg.GraphicsObject] = item_plot_map

        self._x: int = 0
        self._y: int = 0
        self._plot_name: str = ""

        self._init_ui()
        self._connect_signal()

    def _init_ui(self):
        """"""
        self._init_line()
        self._init_label()
        self._init_info()

    def _init_line(self) -> None:
        """
        Create line objects.
        """
        self._v_lines: Dict[str, pg.InfiniteLine] = {}
        self._h_lines: Dict[str, pg.InfiniteLine] = {}
        self._views: Dict[str, pg.ViewBox] = {}

        pen = pg.mkPen(WHITE_COLOR)

        for plot_name, plot in self._plots.items():
            v_line = pg.InfiniteLine(angle=90, movable=False, pen=pen)
            h_line = pg.InfiniteLine(angle=0, movable=False, pen=pen)
            view = plot.getViewBox()

            for line in [v_line, h_line]:
                line.setZValue(0)
                line.hide()
                view.addItem(line)

            self._v_lines[plot_name] = v_line
            self._h_lines[plot_name] = h_line
            self._views[plot_name] = view

    def _init_label(self) -> None:
        """
        Create label objects on axis.
        """
        self._y_labels: Dict[str, pg.TextItem] = {}
        for plot_name, plot in self._plots.items():
            label = pg.TextItem(
                plot_name, fill=CURSOR_COLOR, color=BLACK_COLOR)
            label.hide()
            label.setZValue(2)
            label.setFont(NORMAL_FONT)
            plot.addItem(label, ignoreBounds=True)
            self._y_labels[plot_name] = label

        self._x_label: pg.TextItem = pg.TextItem(
            "datetime", fill=CURSOR_COLOR, color=BLACK_COLOR)
        self._x_label.hide()
        self._x_label.setZValue(2)
        self._x_label.setFont(NORMAL_FONT)
        plot.addItem(self._x_label, ignoreBounds=True)

    def _init_info(self) -> None:
        """
        """
        self._infos: Dict[str, pg.TextItem] = {}
        for plot_name, plot in self._plots.items():
            info = pg.TextItem(
                "info",
                color=CURSOR_COLOR,
                border=CURSOR_COLOR,
                fill=BLACK_COLOR
            )
            info.hide()
            info.setZValue(2)
            info.setFont(NORMAL_FONT)
            plot.addItem(info)  # , ignoreBounds=True)
            self._infos[plot_name] = info

    def _connect_signal(self) -> None:
        """
        Connect mouse move signal to update function.
        """
        self._widget.scene().sigMouseMoved.connect(self._mouse_moved)

    def _mouse_moved(self, evt: tuple) -> None:
        """
        Callback function when mouse is moved.
        """
        if not self._manager.get_count():
            return

        # First get current mouse point
        pos = evt

        for plot_name, view in self._views.items():
            rect = view.sceneBoundingRect()

            if rect.contains(pos):
                mouse_point = view.mapSceneToView(pos)
                self._x = to_int(mouse_point.x())
                self._y = mouse_point.y()
                self._plot_name = plot_name
                break

        # Then update cursor component
        self._update_line()
        self._update_label()
        self.update_info()

    def _update_line(self) -> None:
        """"""
        for v_line in self._v_lines.values():
            v_line.setPos(self._x)
            v_line.show()

        for plot_name, h_line in self._h_lines.items():
            if plot_name == self._plot_name:
                h_line.setPos(self._y)
                h_line.show()
            else:
                h_line.hide()

    def _update_label(self) -> None:
        """"""
        bottom_plot = list(self._plots.values())[-1]
        axis_width = bottom_plot.getAxis("right").width()
        axis_height = bottom_plot.getAxis("bottom").height()
        axis_offset = QtCore.QPointF(axis_width, axis_height)

        bottom_view = list(self._views.values())[-1]
        bottom_right = bottom_view.mapSceneToView(
            bottom_view.sceneBoundingRect().bottomRight() - axis_offset
        )

        for plot_name, label in self._y_labels.items():
            if plot_name == self._plot_name:
                label.setText(str(self._y))
                label.show()
                label.setPos(bottom_right.x(), self._y)
            else:
                label.hide()

        dt = self._manager.get_datetime(self._x)
        if dt:
            self._x_label.setText(dt.strftime("%Y-%m-%d %H:%M:%S"))
            self._x_label.show()
            self._x_label.setPos(self._x, bottom_right.y())
            self._x_label.setAnchor((0, 0))

    def update_info(self) -> None:
        """"""
        buf = {}

        for item, plot in self._item_plot_map.items():
            item_info_text = item.get_info_text(self._x)

            if plot not in buf:
                buf[plot] = item_info_text
            else:
                if item_info_text:
                    buf[plot] += ("\n\n" + item_info_text)

        for plot_name, plot in self._plots.items():
            plot_info_text = buf[plot]
            info = self._infos[plot_name]
            info.setText(plot_info_text)
            info.show()

            view = self._views[plot_name]
            top_left = view.mapSceneToView(view.sceneBoundingRect().topLeft())
            info.setPos(top_left)

    def move_right(self) -> None:
        """
        Move cursor index to right by 1.
        """
        if self._x == self._manager.get_count() - 1:
            return
        self._x += 1

        self._update_after_move()

    def move_left(self) -> None:
        """
        Move cursor index to left by 1.
        """
        if self._x == 0:
            return
        self._x -= 1

        self._update_after_move()

    def move_to(self, x) -> None:
        bar_count = self._manager.get_count()
        if x > bar_count - 1:
            x = bar_count - 1
        elif x < 0:
            x = 0

        self._x = x

        self._update_after_move()

    def _update_after_move(self) -> None:
        """
        Update cursor after moved by left/right.
        """
        bar = self._manager.get_bar(self._x)
        self._y = bar.close_price

        self._update_line()
        self._update_label()

    def clear_all(self) -> None:
        """
        Clear all data.
        """
        self._x = 0
        self._y = 0
        self._plot_name = ""

        for line in list(self._v_lines.values()) + list(self._h_lines.values()):
            line.hide()

        for label in list(self._y_labels.values()) + [self._x_label]:
            label.hide()


class MarketDataChartWidget(ChartWidget):
    signal_new_bar_request = QtCore.pyqtSignal(int)
    def __init__(self, parent: QWidget = None):
        super().__init__(parent)
        self.dt_ix_map = {}
        self.last_ix = -1
        self.ix_trades_map = defaultdict(list)
        self.ix_pos_map = defaultdict(lambda :(0, 0))
        self.ix_holding_pos_map = defaultdict(lambda :(0, 0))
        self.vt_symbol = None
        self.bar = None
        self.last_tick = None
        self._updated = True
        self.indicators = {i.name: i for i in INDICATOR if i.plot_name == 'indicator'}
        self.current_indicator = list(self.indicators.keys())[0]
        self.init_chart_ui()

    def init_chart_ui(self):
        self.add_plot("candle", hide_x_axis=True, minimum_height=200)
        self.add_plot("indicator", hide_x_axis=True, maximum_height=120)
        self.add_plot("pnl", hide_x_axis=True, maximum_height=80)
        self.add_plot("volume", maximum_height=100)

        self.get_plot("candle").showGrid(True, True)
        self.get_plot("indicator").showGrid(True, True)
        self.get_plot("pnl").showGrid(True, True)
        self.get_plot("volume").showGrid(True, True)

        self.add_item(CandleItem, "candle", "candle")

        for i in INDICATOR:
            if i.plot_name == 'candle':
                self.add_item(i, i.name, i.plot_name)

        ind = self.indicators[self.current_indicator]
        self.add_item(ind, ind.name, ind.plot_name)
        self.add_item(PNLCurveItem, PNLCurveItem.name, PNLCurveItem.plot_name)
        self.add_item(VolumeItem, "volume", "volume")
        self.add_cursor()

        self.init_trade_scatter()
        self.init_last_tick_line()
        self.init_order_lines()
        self.init_trade_info()
        self.init_splitLine()

    def init_trade_scatter(self):
        self.trade_scatter = pg.ScatterPlotItem()
        candle_plot = self.get_plot("candle")
        candle_plot.addItem(self.trade_scatter)

    def init_last_tick_line(self):
        self.last_tick_line = pg.InfiniteLine(angle=0, label='')
        candle_plot = self.get_plot("candle")
        candle_plot.addItem(self.last_tick_line)

    def init_order_lines(self):
        self.order_lines = defaultdict(pg.InfiniteLine)

    def init_trade_info(self):
        self.trade_info = pg.TextItem(
                "info",
                anchor=(1, 0),
                color=CURSOR_COLOR,
                border=CURSOR_COLOR,
                fill=BLACK_COLOR
            )
        self.trade_info.hide()
        self.trade_info.setZValue(2)
        self.trade_info.setFont(NORMAL_FONT)

        candle_plot = self.get_plot("candle")
        candle_plot.addItem(self.trade_info)

        self.scene().sigMouseMoved.connect(self.show_trade_info)

    def change_indicator(self, indicator):
        indicator_plot = self.get_plot("indicator")
        if self.current_indicator:
            for item in indicator_plot.items:
                if isinstance(item, ChartItem):
                    indicator_plot.removeItem(item)
                    self._items.pop(self.current_indicator)
                    self._item_plot_map.pop(item)

        self.current_indicator = indicator
        self.add_item(self.indicators[indicator], indicator, "indicator")

        self._items[self.current_indicator].update_history(self._manager.get_all_bars())

    def show_trade_info(self, evt: tuple) -> None:
        info = self.trade_info
        info.hide()
        trades = self.ix_trades_map[self._cursor._x]
        pos = self.ix_pos_map[self._cursor._x]
        holding_pos = self.ix_holding_pos_map[self._cursor._x]
        pos_info_text = f'Pos: {pos[0]}@{pos[1]/pos[0] if pos[0] != 0 else pos[1]:.1f}'
        holding_pos_text = f'Holding: {holding_pos[0]}@{holding_pos[1]/holding_pos[0] if holding_pos[0] != 0 else holding_pos[1]:.1f}'
        trade_info_text = '\n'.join(f'{t.time}: {"↑" if t.direction == Direction.LONG else "↓"}{t.volume}@{t.price:.1f}' for t in trades)
        info.setText('\n'.join([pos_info_text, holding_pos_text, trade_info_text]))
        view = self._cursor._views['candle']
        rect = view.sceneBoundingRect()
        top_middle = view.mapSceneToView(QPointF(rect.right() - rect.width()/2, rect.top()))
        info.setPos(top_middle)
        info.show()

    def update_all(self, history, trades, orders):
        self.update_history(history)
        self.update_trades(trades)
        self.update_orders(orders)
        self.update_pos()
        self.update_pnl()

    def update_history(self, history: list):
        """"""
        with self.updating():
            super().update_history(history)

            if len(history) == 0:
                return

            for ix, bar in enumerate(history):
                self.dt_ix_map[bar.datetime] = ix
            else:
                self.last_ix = ix

    def update_tick(self, tick: TickData):
        """
        Update new tick data into generator.
        """
        new_minute = False

        # Filter tick data with 0 last price
        if not tick.last_price:
            return

        if not self.bar or self.bar.datetime.minute != tick.datetime.minute:
            new_minute = True

        if new_minute:
            self.bar = BarData(
                symbol=tick.symbol,
                exchange=tick.exchange,
                interval=Interval.MINUTE,
                datetime=tick.datetime.replace(second=0),
                gateway_name=tick.gateway_name,
                open_price=tick.last_price,
                high_price=tick.last_price,
                low_price=tick.last_price,
                close_price=tick.last_price,
                open_interest=tick.open_interest
            )
        else:
            self.bar.high_price = max(self.bar.high_price, tick.last_price)
            self.bar.low_price = min(self.bar.low_price, tick.last_price)
            self.bar.close_price = tick.last_price
            self.bar.open_interest = tick.open_interest
            # self.bar.datetime = tick.datetime

        if self.last_tick:
            volume_change = tick.volume - self.last_tick.volume
            self.bar.volume += max(volume_change, 0)

        self.last_tick = tick
        self.update_bar(self.bar)

    def clear_tick(self):
        self.last_tick = None
        self.last_tick_line.setPos(0)

    def update_bar(self, bar: BarData) -> None:
        if bar.datetime not in self.dt_ix_map:
            self.last_ix += 1
            self.dt_ix_map[bar.datetime] = self.last_ix
            self.ix_pos_map[self.last_ix] = self.ix_pos_map[self.last_ix - 1]
            self.ix_holding_pos_map[self.last_ix] = self.ix_holding_pos_map[self.last_ix - 1]
            super().update_bar(bar)
        else:
            candle = self._items.get('candle')
            volume = self._items.get('volume')
            if candle:
                candle.update_bar(bar)

            if volume:
                volume.update_bar(bar)

    def clear_bars(self):
        self.vt_symbol = None
        self.dt_ix_map.clear()
        self.last_ix = -1

    def update_trades(self, trades: list):
        """"""
        trade_scatters = []
        for trade in trades:

            for _dt, ix in self.dt_ix_map.items():
                if trade.time < _dt:
                    self.ix_trades_map[ix - 1].append(trade)
                    scatter = self.__trade2scatter(ix - 1, trade)
                    trade_scatters.append(scatter)
                    break

        self.trade_scatter.setData(trade_scatters)

    def update_trade(self, trade: TradeData):
        ix = self.dt_ix_map.get(trade.time.replace(second=0))
        if ix is not None:
            self.ix_trades_map[ix].append(trade)
            scatter = self.__trade2scatter(ix, trade)
            self.__trade2pos(ix, trade)
            self.trade_scatter.addPoints([scatter])

        for _dt, ix in self.dt_ix_map.items():
            if trade.time < _dt:
                self.ix_trades_map[ix - 1].append(trade)
                scatter = self.__trade2scatter(ix - 1, trade)
                self.__trade2pos(ix-1, trade)
                self.trade_scatter.addPoints([scatter])
                break

    def clear_trades(self):
        self.trade_scatter.clear()
        self.ix_trades_map = defaultdict(list)

    def update_orders(self, orders: list):
        for o in orders:
            self.update_order(o)

    def __trade2scatter(self, ix, trade: TradeData):
        scatter = {
            "pos": (ix, trade.price),
            "data": 1,
            "size": 14,
            "pen": pg.mkPen((255, 255, 255))
        }

        if trade.direction == Direction.LONG:
            scatter["symbol"] = "t1"
            scatter["brush"] = pg.mkBrush((255, 255, 0))
        else:
            scatter["symbol"] = "t"
            scatter["brush"] = pg.mkBrush((0, 0, 255))

        return scatter

    def __trade2pos(self, ix, trade: TradeData):
        if trade.direction == Direction.LONG:
            p = trade.volume
            v = trade.volume * trade.price
        else:
            p = -trade.volume
            v = -trade.volume * trade.price
        self.ix_pos_map[ix] = (self.ix_pos_map[ix][0] + p, self.ix_pos_map[ix][1] + v)

        if self.ix_pos_map[ix][0] == 0:
            self.ix_holding_pos_map[ix] = (0, 0)
        else:
            self.ix_holding_pos_map[ix] = (self.ix_holding_pos_map[ix][0] + p, self.ix_holding_pos_map[ix][1] + v)

    def update_order(self, order: OrderData):
        if order.status in (Status.NOTTRADED, Status.PARTTRADED):
            line = self.order_lines[order.vt_orderid]
            candle_plot = self.get_plot("candle")

            if line not in candle_plot.items:
                candle_plot.addItem(line)

            line.setAngle(0)
            line.label = pg.InfLineLabel(line,
                                         text=f'{order.type.value}:{"↑" if order.direction == Direction.LONG else "↓"}{order.volume - order.traded}@{order.price}',
                                         color='r' if order.direction == Direction.LONG else 'g')
            line.setPen(pg.mkPen(color=UP_COLOR if order.direction == Direction.LONG else DOWN_COLOR, width=PEN_WIDTH))
            line.setHoverPen(pg.mkPen(color=UP_COLOR if order.direction == Direction.LONG else DOWN_COLOR, width=PEN_WIDTH * 2))
            line.setPos(order.price)

        elif order.status in (Status.ALLTRADED, Status.CANCELLED, Status.REJECTED):
            if order.vt_orderid in self.order_lines:
                line = self.order_lines[order.vt_orderid]
                candle_plot = self.get_plot("candle")
                candle_plot.removeItem(line)

    def clear_orders(self):
        candle_plot = self.get_plot("candle")
        for _, l in self.order_lines.items():
            candle_plot.removeItem(l)

        self.order_lines.clear()

    def update_tick_line(self, tick: TickData):
        c = tick.last_price
        o = self.bar.close_price if self.bar else c
        self.last_tick_line.setPos(c)
        if c >= o:
            self.last_tick_line.setPen(pg.mkPen(color=UP_COLOR, width=PEN_WIDTH/2))
            self.last_tick_line.label.setText(str(c), color=(255, 69, 0))
        else:
            self.last_tick_line.setPen(pg.mkPen(color=DOWN_COLOR, width=PEN_WIDTH / 2))
            self.last_tick_line.label.setText(str(c), color=(173, 255, 47))

    def update_pos(self):
        net_p = 0
        net_value = 0
        holding_value = 0
        for ix in self.dt_ix_map.values():
            trades = self.ix_trades_map[ix]
            for t in trades:
                if t.direction == Direction.LONG:
                    net_p += t.volume
                    net_value += t.volume * t.price
                    holding_value += t.volume * t.price
                else:
                    net_p -= t.volume
                    net_value -= t.volume * t.price
                    holding_value -= t.volume * t.price
            else:
                if net_p == 0:
                    holding_value = 0
            self.ix_pos_map[ix] = (net_p, net_value)
            self.ix_holding_pos_map[ix] = (net_p, holding_value)

    def clear_pos(self):
        self.ix_pos_map = defaultdict(lambda :(0, 0))
        self.ix_holding_pos_map = defaultdict(lambda :(0, 0))

    def update_pnl(self):
        pnl_plot = self._plots.get('pnl')
        pnl_item = self._items.get('pnl')
        if pnl_plot and pnl_item:
            pnl_item.clear_all()
            pnl_item.set_ix_pos_map(self.ix_pos_map)
            pnl_item.update_history(self._manager.get_all_bars())

            min_value, max_value = pnl_item.get_y_range()

            pnl_plot.setLimits(
                xMin=-1,
                xMax=self._manager.get_count(),
                yMin=min_value,
                yMax=max_value
            )


    def init_splitLine(self):
        self.splitLines = []

    def add_splitLine(self, split_dt, style=None, offset=-0.5):
        candle = self.get_plot('candle')
        ix = self.dt_ix_map.get(split_dt, None)
        if candle and ix is not None:
            sl = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen(color='r', width=1.5, style=style if style else QtCore.Qt.DashDotLine))
            sl.setPos(ix + offset)
            candle.addItem(sl)
            self.splitLines.append(sl)

    def clear_splitLine(self):
        candle = self.get_plot('candle')
        if candle:
            for l in self.splitLines:
                candle.removeItem(l)
            else:
                self.splitLines.clear()

    def keyPressEvent(self, event: QtGui.QKeyEvent) -> None:
        """
        Reimplement this method of parent to move chart horizontally and zoom in/out.
        """
        super().keyPressEvent(event)

        if event.key() == QtCore.Qt.Key_PageUp:
            self._on_key_pageUp()
        elif event.key() == QtCore.Qt.Key_PageDown:
            self._on_key_pageDown()

    def _on_key_pageUp(self):
        x = self._cursor._x
        while x <= self._right_ix:
            x += 1
            if self.ix_trades_map.get(x):
                self._cursor.move_to(x)
                self.show_trade_info(tuple())
                break

    def _on_key_pageDown(self):
        x = self._cursor._x
        while x >= 0:
            x -= 1
            if self.ix_trades_map.get(x):
                self._cursor.move_to(x)
                self.show_trade_info(tuple())
                break

    def mouseMoveEvent(self, ev):
        super().mouseMoveEvent(ev)

        if ev.buttons() != Qt.LeftButton:
            return

        last_x = self._mouse_last_x
        cur_x = ev.x()
        self._mouse_last_x = ev.x()
        offset = last_x - cur_x
        if self.is_updated() and offset >= 15 and self._right_ix >= self.last_ix:
            self.signal_new_bar_request.emit(offset)

    def mousePressEvent(self, ev):
        super().mousePressEvent(ev)

        if ev.buttons() != Qt.LeftButton:
            return

        self._mouse_last_x = ev.x()

    # def mouseReleaseEvent(self, ev):
    #     super().mouseReleaseEvent(ev)
    #
    #     if ev.buttons() != Qt.LeftButton:
    #         return
    #
    #     self._mouse_last_x = None
    #     self._allow_new_bar_request = False

    def is_updated(self):
        return self._updated

    @contextmanager
    def updating(self):
        self._updated = False
        yield self
        self._updated = True

    def clear_all(self) -> None:
        """"""
        super().clear_all()
        self.clear_bars()
        self.clear_trades()
        self.clear_pos()
        self._updated = True

        self.clear_orders()
        self.clear_tick()
        self.clear_splitLine()