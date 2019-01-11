#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/4 0004 9:24
# @Author  : Hadrianl 
# @File    : CTPTickRecorder


from ctpwrapper import MdApiPy, TraderApiPy
from ctpwrapper import ApiStructure
import pymongo as pm
from copy import copy
from queue import Queue
from sys import float_info
from dateutil import parser

class CTPTickRecorder(MdApiPy):
    def __init__(self):
        self._sub_list = {}
        self._ticker_queue = Queue()

    def connectCTP(self, FrontUrl_MD, FrontUrl_TD, investor_id, password, broker_id, request_id=1):
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.password = password
        self.request_id = request_id
        self.td_front = FrontUrl_TD
        self.Create()
        self.RegisterFront(FrontUrl_MD)
        self.Init()

    def connectDB(self, user, pwd, host, port=27017):
        client = pm.MongoClient(host, port)
        auth_db = client.get_database('admin')
        auth_db.authenticate(user, pwd)
        self._db = client.get_database('CTPData')

    def get_ins_list(self,):
        self.td = td = Trader(self.broker_id, self.investor_id, self.password)
        td.Create()
        td.RegisterFront(self.td_front)
        td.SubscribePublicTopic(2)
        td.SubscribePrivateTopic(2)
        td.Init()
        col = self._db.get_collection('Instruments')
        col.create_index([('InstrumentID', pm.ASCENDING)], unique=True)
        while True:
            ins = td._ins_queue.get()
            if ins:
                ins_ = ins.to_dict()
                ins_['CreateDate'] = parser.parse(ins_['CreateDate'])
                ins_['OpenDate'] = parser.parse(ins_['OpenDate'])
                ins_['ExpireDate'] = parser.parse(ins_['ExpireDate'])
                ins_['StartDelivDate'] = parser.parse(ins_['StartDelivDate'])
                ins_['EndDelivDate'] = parser.parse(ins_['EndDelivDate'])
                col.replace_one({'InstrumentID': ins_['InstrumentID']}, ins_, upsert=True)
                self._sub_list[ins.InstrumentID] = ins
            else:
                userlogout = ApiStructure.UserLogoutField(self.broker_id, self.investor_id)
                td.ReqUserLogout(userlogout, td.inc_request_id())
                break
        return self._sub_list

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        self.ErrorRspInfo(pRspInfo, nRequestID)

    def ErrorRspInfo(self, info, request_id):
        """
        :param info:
        :return:
        """
        if info.ErrorID != 0:
            print('request_id=%s ErrorID=%d, ErrorMsg=%s',
                  request_id, info.ErrorID, info.ErrorMsg.decode('gbk'))
        return info.ErrorID != 0

    def OnFrontConnected(self):
        """
        :return:
        """

        user_login = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
                                                    UserID=self.investor_id,
                                                    Password=self.password)
        self.ReqUserLogin(user_login, self.request_id)

    def OnFrontDisconnected(self, nReason):

        print("Md OnFrontDisconnected %s", nReason)
        # sys.exit()

    def OnHeartBeatWarning(self, nTimeLapse):
        """心跳超时警告。当长时间未收到报文时，该方法被调用。
        @param nTimeLapse 距离上次接收报文的时间
        """
        print('Md OnHeartBeatWarning, time = %s', nTimeLapse)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """
        用户登录应答
        :param pRspUserLogin:
        :param pRspInfo:
        :param nRequestID:
        :param bIsLast:
        :return:
        """
        if pRspInfo.ErrorID != 0:
            print("Md OnRspUserLogin failed error_id=%s msg:%s",
                  pRspInfo.ErrorID, pRspInfo.ErrorMsg.decode('gbk'))
        else:
            print("Md user login successfully")
            print(pRspUserLogin)
            print(pRspInfo)
            sub_list = self.get_ins_list()
            print('sub_list:', list(sub_list))
            super(MdApiPy, self).SubscribeMarketData(list(sub_list))
                # self.SubscribeMarketData(list(self._sub_list))

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID == 0:
            print(f'<{pSpecificInstrument.InstrumentID}>SubMarketData Succeed!')
        else:
            print(pSpecificInstrument, pRspInfo, nRequestID, bIsLast)

    def OnRspUnSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID == 0:
            try:
                print(f'<{pSpecificInstrument.InstrumentID}>UnSubMarketData Succeed!')
            except KeyError:
                print(f'<{pSpecificInstrument.InstrumentID}> Not Exist!')
        else:
            print(pSpecificInstrument, pRspInfo, nRequestID, bIsLast)

    def OnRtnDepthMarketData(self, pDepthMarketData):
        data = copy(pDepthMarketData)
        self._ticker_queue.put(data)

    def SubscribeMarketData(self, pInstrumentID: list):
        ids = [bytes(item, encoding="utf-8") for item in pInstrumentID]
        return super(MdApiPy, self).SubscribeMarketData(ids)

    def UnSubscribeMarketData(self, pInstrumentID: list):
        ids = [bytes(item, encoding="utf-8") for item in pInstrumentID]
        return super(MdApiPy, self).SubscribeMarketData(ids)


    def RecordTicker(self):
        print('开启RecordTicker')
        while True:
            try:
                data = self._ticker_queue.get()
                self.SaveTicker(data)
            except KeyboardInterrupt:
                print('关闭RecordTicker')
                break
            except Exception as e:
                print(e)

    def SaveTicker(self, pDepthMarketData):
        data = pDepthMarketData.to_dict()
        for i in range(2,6):
            for key in ['BidPrice', 'BidVolume', 'AskPrice', 'AskVolume']:
                data.pop(f'{key}{i}')

        for k in data:
            if data[k] == float_info.max:
                data[k] = 0.0

        col = self._db.get_collection(data['InstrumentID'])
        col.insert_one(data)


class Trader(TraderApiPy):

    def __init__(self, broker_id, investor_id, password, request_id=1):
        self.request_id = request_id
        self.broker_id = broker_id.encode()
        self.investor_id = investor_id.encode()
        self.password = password.encode()
        self._ins_queue = Queue()

    def inc_request_id(self):
        self.request_id += 1
        return self.request_id

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        self.ErrorRspInfo(pRspInfo, nRequestID)

    def ErrorRspInfo(self, info, request_id):
        if info.ErrorID != 0:
            print('request_id=%s ErrorID=%d, ErrorMsg=%s',
                  request_id, info.ErrorID, info.ErrorMsg.decode('gbk'))
        return info.ErrorID != 0

    def OnFrontDisconnected(self, nReason):
        print("on FrontDisConnected disconnected", nReason)

    def OnFrontConnected(self):

        req = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
                                             UserID=self.investor_id,
                                             Password=self.password)
        self.ReqUserLogin(req, self.request_id)
        print("trader on front connection")

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):

        if pRspInfo.ErrorID != 0:
            print("td OnRspUserLogin failed error_id=%s msg:%s",
                  pRspInfo.ErrorID, pRspInfo.ErrorMsg.decode('gbk'))
        else:
            print("td user login successfully")
            qryinstrument = ApiStructure.QryInstrumentField()
            self.ReqQryInstrument(qryinstrument, self.inc_request_id())


    def OnRspUserLogout(self, pUserLogout, pRspInfo, nRequestID, bIsLast):
        print('td user logout successfully')


    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        self._ins_queue.put(copy(pInstrument) if not bIsLast else None)

if __name__ == '__main__':
    data_recorder = CTPTickRecorder()
    data_recorder.connectDB('kairuitouzi', '', '192.168.2.226', 27017)
    data_recorder.connectCTP('tcp://180.168.146.187:10010', 'tcp://180.168.146.187:10000', '120324', '127565568yjd', '9999')
    data_recorder.RecordTicker()