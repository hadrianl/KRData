#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
@author:Hadrianl
THANKS FOR th github project https://github.com/moonnejs/uiKLine
"""

import numpy as np
import numba
import pandas as pd
import datetime as dt
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *
from PyQt5 import QtCore
from ..HKData import HKMarket
from ..IBData import IBMarket, IBTrade
from ..util import _concat_executions, load_json_settings, save_json_settings
from typing import Iterable
from dateutil import parser
from PyQt5 import QtWidgets
from .baseObject import *
from .baseQtItems import *


EVENT_BAR_UPDATE = 'eBarUpdate'
DEFAULT_MA_SETTINGS = {'5': 'r', '10': 'b', '30': 'g', '60': 'm'}
DEFAULT_MACD_SETTINGS = {'fastperiod': 12, 'slowperiod': 26, 'signalperiod': 9}
DEFAULT_TRADE_MARK_SETTINGS = {'long': {'angle': 90, 'brush': 'b', 'headLen': 15}, 'short': {'angle': -90, 'brush': 'y', 'headLen': 15}}
DEFAULT_SYMBOL_SETTINGS = ''
DEFAULT_ACCOUNT_SETTINGS = ''
SETTINGS = load_json_settings('visual_settings.json')
MA_SETTINGS = SETTINGS.get('MA', DEFAULT_MA_SETTINGS)
TRADE_MARK_SETTINGS = SETTINGS.get('TradeMark', DEFAULT_TRADE_MARK_SETTINGS)
MACD_SETTINGS = SETTINGS.get('MACD', DEFAULT_MACD_SETTINGS)
SYMBOL_SETTINGS = SETTINGS.get('SYMBOL', DEFAULT_SYMBOL_SETTINGS)
ACCOUNT_SETTINGS = SETTINGS.get('ACCOUNT', DEFAULT_ACCOUNT_SETTINGS)
MAX_LEN = 3000




class KLineWidget(QtWidgets.QWidget):

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

        # self.splitLines = []
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
        self.chart = MarketDataChartWidget()
        # 设置界面
        self.vb = QVBoxLayout()

        self.symbol_line = QLineEdit(SYMBOL_SETTINGS)
        self.symbol_line.setPlaceholderText('输入产品代码如: HSI1907或者369009605')
        # self.symbol_line.returnPressed.connect(self.subscribe) # todo

        self.interval_combo = QComboBox()
        for inteval in ['1min', '5min', '15min', '30min', '60min', '1day']:
            self.interval_combo.addItem(inteval)

        self.indicator_combo = QtWidgets.QComboBox()
        self.indicator_combo.addItems([n for n in self.chart.indicators.keys()])
        self.indicator_combo.currentTextChanged.connect(self.chart.change_indicator)

        # self.export_btn = QPushButton('保存')
        # self.export_btn.clicked.connect(self.export_image)

        self.interval_combo.currentTextChanged.connect(self.change_querier_period)
        data_params_layout = QHBoxLayout()
        data_params_layout.addWidget(self.symbol_line)
        data_params_layout.addWidget(self.interval_combo)
        data_params_layout.addWidget(self.indicator_combo)
        data_params_layout.addStretch()
        # data_params_layout.addWidget(self.export_btn)
        self.data_params_layout = data_params_layout


        self.datetime_from = QDateTimeEdit()
        self.datetime_to = QDateTimeEdit()
        self.datetime_from.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        self.datetime_to.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        self.datetime_from.setCalendarPopup(True)
        self.datetime_to.setCalendarPopup(True)

        # self.trade_links = QCheckBox("Trade Link")
        # self.trade_links.stateChanged.connect(self.refreshTradeLinks)

        now = dt.datetime.now()
        self.datetime_from.setDateTime(now - dt.timedelta(days=1))
        self.datetime_to.setDateTime(now)
        timerange_layout = QHBoxLayout()
        timerange_layout.addWidget(self.datetime_from)
        timerange_layout.addWidget(self.datetime_to)
        # timerange_layout.addWidget(self.trade_links)
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

        self.vb.addWidget(self.chart)
        self.setLayout(self.vb)
        self.resize(1300, 700)
        # 初始化完成
        self.initCompleted = True
        # self.query_data()
        self.chart.signal_new_bar_request.connect(self.update_bars_backward)

        # ----------------------------------------------------------------------
    def closeEvent(self, a0: QCloseEvent) -> None:
        super().closeEvent(a0)
        SETTINGS['SYMBOL'] = self.symbol_line.text()
        SETTINGS['ACCOUNT'] = self.account_line.text()
        save_json_settings('visual_settings.json', SETTINGS)

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

    def query_data(self):
        # self.clearData()
        self.chart.clear_all()
        start = self.datetime_from.dateTime().toPyDateTime()
        end = self.datetime_to.dateTime().toPyDateTime()
        symbol = self.symbol_line.text()

        try:
            if self.data_source == 'HK':
                query_set1 = self._querier[120:start:symbol]
                query_set2 = self._querier[start:end:symbol]
                query_set3 = self._querier[end:120:symbol]
                data1 = self._querier.to_df(query_set1)
                data2 = self._querier.to_df(query_set2)
                data3 = self._querier.to_df(query_set3)
                datas = pd.concat([data1, data2, data3])

            elif self.data_source == 'IB':
                symbol = int(symbol) if symbol.isdigit() else symbol
                contract = self._querier.verifyContract(symbol)
                barType = {'1min': '1 min', '5min': '5 mins', '15min': '15 mins', '30min': '30 mins', '60min': '60 mins', '1day': '1 day'}.get(self.period,'1 min')
                datas = self._querier.get_bars_from_ib(contract, barType=barType, start=start, end=end)
        except Exception as e:
            QMessageBox.critical(self,'获取数据错误', str(e))
            return

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

        self.datas = datas
        barList = []
        tradeList = []
        for _, d in datas.iterrows():
            b = BarData('KRData', symbol, Exchange.HKFE, d.datetime, None,
                        d.volume, 0, d.open, d.high, d.low, d.close)
            barList.append(b)

        for e in executions:
            t = TradeData('KRData', symbol, Exchange.HKFE, '', '',
                          Direction.LONG if e['direction'] == 'long' else Direction.SHORT,
                          Offset.NONE,
                          e['price'],
                          e['size'],
                          e['datetime'] if isinstance(e['datetime'], dt.datetime) else parser.parse(e['datetime'])
                          )
            tradeList.append(t)

        self.chart.update_all(barList, tradeList, [])

    def update_bars_backward(self, n):
        if self.data_source == 'HK':
            with self.chart.updating() as chart:
                symbol = self.symbol_line.text()
                start = chart._manager.get_bar(chart.last_ix).datetime
                data = self._querier[start:n:symbol]

                for d in data:
                    b = BarData('KRData', symbol, Exchange.HKFE, d.datetime, None,
                                d.volume, 0, d.open, d.high, d.low, d.close)
                    chart.update_bar(b)
        elif self.data_source == 'IB':
            with self.chart.updating() as chart:
                symbol = self.symbol_line.text()
                barType = {'1min': '1 min', '5min': '5 mins', '15min': '15 mins', '30min': '30 mins', '60min': '60 mins', '1day': '1 day'}.get(self.period,'1 min')
                minutes = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '60min': 60, '1day': 1440}.get(self.period, 1)
                start = chart._manager.get_bar(chart.last_ix).datetime
                per_bar_period = dt.timedelta(minutes=minutes)
                if dt.datetime.now() - start <= per_bar_period:
                    return
                n = min(60, n)
                contract = self._querier.verifyContract(symbol)
                data = self._querier.get_bars_from_ib(contract, barType=barType, start=start, end=start + per_bar_period * n)
                for _, d in data.iterrows():
                    b = BarData('KRData', symbol, Exchange.HKFE, d.datetime, None,
                                d.volume, 0, d.open, d.high, d.low, d.close)
                    chart.update_bar(b)

    def setExecutions(self, executions:list):
        self.executions = executions
        if self.executions:
            self.executions.sort(key=lambda t: t['datetime'])

    def change_querier_period(self, period):
        self.period = period
        if self.data_source == 'HK':
            self._querier = HKMarket(period=period)

    def refreshTradeLinks(self, b):
        if b:
            for l in self.tradeLines:
                self.pwKL.addItem(l)
        else:
            for l in self.tradeLines:
                self.pwKL.removeItem(l)


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
            if extension == '.pkl':
                with open(fname[0], 'rb') as f:
                    executions_list = pickle.load(f)
                    self.executions = pd.DataFrame(executions_list)
            else:
                self.executions = pd.read_excel(fname[0])

            if 'tradeDate' not in self.executions.columns:
                self.executions['tradeDate'] = self.executions.datetime.apply(lambda d: str(d.date()))
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
        # self.klineWidget.pwKL.removeItem(self.klineWidget.candle.tickLine)
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
        print(self.trades)
        for k1 in self.trades[0]:
            if isinstance(self.trades[0][k1], dict):
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
                cell.setData(Qt.DisplayRole, v if isinstance(v, (int, float)) else str(v))

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
            if extension == '.pkl':
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
                            d[splited[0]] = t[i]
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
            end = end if isinstance(end, dt.datetime) else parser.parse(end)
            start = start - dt.timedelta(days=1)
            end = end + dt.timedelta(days=1)

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

class CorrelationMonitor(QWidget):
    signal_new_corr = QtCore.pyqtSignal((dt.datetime, float, float))
    signal_new_dtw = QtCore.pyqtSignal((dt.datetime, float))
    signal_process = QtCore.pyqtSignal(int)

    class DataFetcher(QThread):
        def __init__(self, parent=None):
            super().__init__(parent)
            from KRData.HKData import HKFuture
            self.hf = HKFuture()
            self.finished.connect(self.parent().show_data_fetch_finished)

        def run(self) -> None:
            parent = self.parent()
            if parent:
                parent.raw_data['HSI'] = self.hf.get_main_contract_bars('HSI', start='20190101')

    def __init__(self):
        super().__init__()
        self.interval = 1
        self.raw_data = {}
        self.period = 0
        self._selected_row = 0
        self.data_fetcher = self.DataFetcher(self)
        self.hkm = HKMarket()
        self.data_fetcher.start()

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle('相关性对比')
        self.setAcceptDrops(True)
        self.target_chart_widget = KLineWidget()
        # self.target_chart_widget.interval_combo.setEnabled(False)
        self.source_chart_widget = MarketDataChartWidget()
        self.compare_corr_btn = QPushButton('计算corr')
        self.compare_dtw_btn = QPushButton('计算dtw')
        self.interval_combo = QComboBox()
        for i in [1, 5, 10, 15, 30, 60]:
            self.interval_combo.addItem(f'{i} min', i)

        self.save_btn = QPushButton('保存数据')

        # self.compare_btn.clicked.connect(self.calc_corr)
        # self.forward_num = QtWidgets.QLineEdit('30')
        # self.target_chart_widget.indicator_combo.currentTextChanged.connect(self.source_chart_widget.change_indicator)

        self.info_table = QTableWidget()
        self.info_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.info_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.info_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.info_table.verticalHeader().setVisible(False)
        self.info_table.setSortingEnabled(True)
        # self.corr_table.cellDoubleClicked.connect(self.show_chart)

        self.process_bar = QtWidgets.QProgressBar()

        self.vb = QHBoxLayout()
        inner_vb = QVBoxLayout()

        inner_vb.addWidget(self.target_chart_widget)
        btn_hbox = QHBoxLayout()
        btn_hbox.addWidget(self.compare_corr_btn, 2)
        btn_hbox.addWidget(self.compare_dtw_btn, 2)
        btn_hbox.addWidget(self.interval_combo, 1)
        # btn_hbox.addWidget(self.forward_num, 2)
        btn_hbox.addWidget(self.process_bar, 5)
        btn_hbox.addWidget(self.save_btn, 1)
        inner_vb.addLayout(btn_hbox)
        inner_vb.addWidget(self.source_chart_widget)

        self.vb.addLayout(inner_vb, 8)
        self.vb.addWidget(self.info_table, 2)

        self.setLayout(self.vb)
        self.resize(1500, 1000)

        self.init_signal()

    def init_signal(self):
        def change_interval(index):
            val = self.interval_combo.itemData(index)
            self.interval = val
            self.show_chart(self._selected_row, 0)

        self.interval_combo.currentIndexChanged.connect(change_interval)
        self.compare_corr_btn.clicked.connect(self.calc_corr)
        self.compare_dtw_btn.clicked.connect(self.calc_dtw)
        self.target_chart_widget.indicator_combo.currentTextChanged.connect(self.source_chart_widget.change_indicator)
        self.info_table.cellDoubleClicked.connect(self.show_chart)
        self.source_chart_widget.signal_new_bar_request.connect(self.update_bars_backward)
        self.signal_process.connect(self.process_bar.setValue)
        self.signal_new_corr.connect(self.insert_corr)
        self.signal_new_dtw.connect(self.insert_dtw)
        self.save_btn.clicked.connect(self.save_corr_data)

    def update_bars_backward(self, n):
        source_data = self.raw_data['HSI']
        last_bar = self.bar_generator.last_bar
        if last_bar is None:
            return
        start = last_bar.datetime
        end = source_data.datetime.shift(-n)[start]
        data = source_data[start:end]

        for _, d in data.iterrows():
            b = BarData('KRData', d.code, Exchange.HKFE, d.datetime, None,
                        d.volume, 0, d.open, d.high, d.low, d.close)
            self.bar_generator.update_bar(b)

    def calc_corr(self):
        if not self.data_fetcher.isFinished():
            QMessageBox.critical(self, 'DataFetcher', '数据未加载完毕！', QMessageBox.Ok)
            return
        import scipy.stats as st
        target_data = self.target_chart_widget.datas
        source_data = self.raw_data['HSI']

        apply_func_dict = {'datetime': 'last',
                           'code': 'first',
                           'open': 'first',
                           'high': 'max',
                           'low': 'min',
                           'close': 'last',
                           'volume': 'sum',
                           'trade_date': 'first'
                           }

        ktype = {'1min': '1T', '5min': '5T', '15min': '15T',
                 '30min': '30T', '60min': '60T', '1day': '1D'}.get(self.target_chart_widget.period)
        if ktype:
            self.data = source_data.resample(ktype).apply(apply_func_dict)
            self.data.dropna(thresh=2, inplace=True)
            self.data.datetime = self.data.index
        else:
            self.data = source_data

        target_values = target_data.close.values
        self.process_bar.setValue(0)
        @numba.jit
        def corr(nd):
            return st.pearsonr(nd, target_values)

        self.period = len(target_data)
        # ret = source_data.close.rolling(self.period).apply(corr)

        table = self.info_table
        table.clearContents()
        table.setColumnCount(3)
        # table.setRowCount(len(ret))
        table.setHorizontalHeaderLabels(['datetime', 'corr', 'p'])
        count = len(self.data)

        corr_data = []
        for i in range(self.period, count, 10):
            data = (self.data.datetime[i], *corr(self.data.close.values[i - self.period:i]))
            process_value = (i + 10) * 100 // count
            self.signal_process.emit(process_value)
            self.signal_new_corr.emit(*data)
            corr_data.append(data)
        else:
            self.corr_data = pd.DataFrame(corr_data, columns=['datetime', 'corr', 'p'])

    def calc_dtw(self):
        if not self.data_fetcher.isFinished():
            QMessageBox.critical(self, 'DataFetcher', '数据未加载完毕！', QMessageBox.Ok)
            return
        from tslearn.metrics import soft_dtw
        target_data = self.target_chart_widget.datas
        source_data = self.raw_data['HSI']

        apply_func_dict = {'datetime': 'last',
                           'code': 'first',
                           'open': 'first',
                           'high': 'max',
                           'low': 'min',
                           'close': 'last',
                           'volume': 'sum',
                           'trade_date': 'first'
                           }

        ktype = {'1min': '1T', '5min': '5T', '15min': '15T',
                 '30min': '30T', '60min': '60T', '1day': '1D'}.get(self.target_chart_widget.period)
        if ktype:
            self.data = source_data.resample(ktype).apply(apply_func_dict)
            self.data.dropna(thresh=2, inplace=True)
            self.data.datetime = self.data.index
        else:
            self.data = source_data

        target_values = target_data.close.values
        self.process_bar.setValue(0)

        self.period = len(target_data)
        # ret = source_data.close.rolling(self.period).apply(corr)

        table = self.info_table
        table.clearContents()
        table.setColumnCount(2)
        # table.setRowCount(len(ret))
        table.setHorizontalHeaderLabels(['datetime', 'dtw'])
        count = len(self.data)

        dtw_data = []
        for i in range(count - 1, self.period - 1, -self.period):
            data = (self.data.datetime[i], soft_dtw(target_values, self.data.close.values[i - self.period:i]))
            process_value = (i - self.period) * 100 // count
            self.signal_process.emit(process_value)
            self.signal_new_dtw.emit(*data)
            dtw_data.append(data)
        else:
            self.dtw_data = pd.DataFrame(dtw_data, columns=['datetime', 'dtw'])

    def insert_corr(self, d, corr, p):
        table = self.info_table
        r = table.rowCount()
        table.insertRow(r)
        dt_cell = QTableWidgetItem()
        dt_cell.setFlags(QtCore.Qt.ItemIsEnabled)
        dt_cell.setData(Qt.DisplayRole, str(d))
        dt_cell.setTextAlignment(QtCore.Qt.AlignCenter)

        corr_cell = QTableWidgetItem()
        corr_cell.setFlags(QtCore.Qt.ItemIsEnabled)
        corr_cell.setData(Qt.DisplayRole, 0 if np.isnan(corr) else corr)
        corr_cell.setTextAlignment(QtCore.Qt.AlignCenter)

        p_cell = QTableWidgetItem()
        p_cell.setFlags(QtCore.Qt.ItemIsEnabled)
        p_cell.setData(Qt.DisplayRole, p)
        p_cell.setTextAlignment(QtCore.Qt.AlignCenter)


        table.setItem(r, 0, dt_cell)
        table.setItem(r, 1, corr_cell)
        table.setItem(r, 2, p_cell)

    def insert_dtw(self, d, dtw):
        table = self.info_table
        r = table.rowCount()
        table.insertRow(r)
        dt_cell = QTableWidgetItem()
        dt_cell.setFlags(QtCore.Qt.ItemIsEnabled)
        dt_cell.setData(Qt.DisplayRole, str(d))
        dt_cell.setTextAlignment(QtCore.Qt.AlignCenter)

        dtw_cell = QTableWidgetItem()
        dtw_cell.setFlags(QtCore.Qt.ItemIsEnabled)
        dtw_cell.setData(Qt.DisplayRole, 0 if np.isnan(dtw) else dtw)
        dtw_cell.setTextAlignment(QtCore.Qt.AlignCenter)

        table.setItem(r, 0, dt_cell)
        table.setItem(r, 1, dtw_cell)

    def show_chart(self, r, c):
        self._selected_row = r
        _end = self.info_table.item(r, 0).text()

        source_data = self.raw_data['HSI']
        forward = 1
        start = self.data.shift(self.period).asof(_end).datetime
        end = self.data.shift(-forward).asof(_end).datetime
        data = source_data.loc[start:end]
        self.source_chart_widget.clear_all()
        # barList = []
        if self.interval >= 60:
            self.bar_generator = BarGenerator(None, 1, self.source_chart_widget.update_bar, Interval.HOUR)
        else:
            self.bar_generator = BarGenerator(None, self.interval, self.source_chart_widget.update_bar, Interval.MINUTE)

        for _, d in data.iterrows():
            b = BarData('KRData', d.code, Exchange.HKFE, d.datetime, None,
                        d.volume, 0, d.open, d.high, d.low, d.close)
            self.bar_generator.update_bar(b)
            if d.datetime == parser.parse(_end):
                last_bar = self.source_chart_widget._manager.get_bar(self.source_chart_widget.last_ix)
                self.source_chart_widget.add_splitLine(last_bar.datetime)
            # barList.append(b)
        # self.source_chart_widget.update_all(barList, [], [])
        # self.source_chart_widget.add_splitLine(barList[-forward].datetime, offset=0.5)

    def save_corr_data(self):
        if not hasattr(self, 'self.corr_data'):
            return

        target_data = self.target_chart_widget.datas
        target_start = target_data.iloc[0].datetime
        target_end = target_data.iloc[-1].datetime

        s = {}
        s['start'] = target_start
        s['end'] = target_end
        s['symbol'] = self.target_chart_widget.symbol_line.text()
        s['period'] = self.target_chart_widget.period
        s['source'] = self.target_chart_widget.data_source
        s['data'] = self.corr_data

        # data.to_excel(f'{target_start}-{target_end}.xls', sheet_name=self.target_chart_widget.period)
        import pickle
        with open(f'{target_start.strftime("%Y%m%dT%H%M%S")}_{target_end.strftime("%Y%m%dT%H%M%S")}.pkl', 'wb') as f:
            pickle.dump(s, f)

    def load_corr_data(self, file_name):
        if not self.data_fetcher.isFinished():
            QMessageBox.critical(self, 'DataFetcher', '数据未加载完毕！', QMessageBox.Ok)
            return

        import pickle
        with open(file_name, 'rb') as f:
            s = pickle.load(f)

        self.target_chart_widget.symbol_line.setText(s['symbol'])
        self.target_chart_widget.datetime_from.setDateTime(s['start'])
        self.target_chart_widget.datetime_to.setDateTime(s['end'])
        self.target_chart_widget.interval_combo.setCurrentText(s['period'])
        if s['source'] == 'HK':
            self.target_chart_widget.source_HK_btn.setChecked(True)
        elif s['source'] == 'IB':
            self.target_chart_widget.source_IB_btn.setChecked(True)

        self.info_table.clearContents()
        self.info_table.setColumnCount(3)
        # table.setRowCount(len(ret))
        self.info_table.setHorizontalHeaderLabels(['datetime', 'corr', 'p'])

        for _, d in s['data'].iterrows():
            self.insert_corr(d['datetime'], d['corr'], d['p'])

        source_data = self.raw_data['HSI']

        apply_func_dict = {'datetime': 'last',
                           'code': 'first',
                           'open': 'first',
                           'high': 'max',
                           'low': 'min',
                           'close': 'last',
                           'volume': 'sum',
                           'trade_date': 'first'
                           }

        ktype = {'1min': '1T', '5min': '5T', '15min': '15T',
                 '30min': '30T', '60min': '60T', '1day': '1D'}.get(self.target_chart_widget.period)
        if ktype:
            self.data = source_data.resample(ktype).apply(apply_func_dict)
            self.data.dropna(thresh=2, inplace=True)
            self.data.datetime = self.data.index
        else:
            self.data = source_data

        self.target_chart_widget.query_btn.click()
        self.period = len(self.target_chart_widget.datas)

    def show_data_fetch_finished(self):
        QMessageBox.information(self, 'DataFetcher', '数据加载完毕！', QMessageBox.Ok)

    def closeEvent(self, a0: QCloseEvent) -> None:
        self.target_chart_widget.closeEvent(a0)
        self.source_chart_widget.closeEvent(a0)
        super().closeEvent(a0)

    def dragEnterEvent(self, a0: QDragEnterEvent) -> None:
        super().dragEnterEvent(a0)
        if a0.mimeData().hasFormat('text/plain'):
            a0.accept()
        else:
            a0.ignore()

    def dropEvent(self, a0: QDropEvent) -> None:
        super().dropEvent(a0)

        file_name = a0.mimeData().text().lstrip('file:/')
        self.load_corr_data(file_name)
