#
# 采集仪器信息
# 采集仪器的数采、当天数据、实时数据
'''
    测试采集
    类名DataCollection
    ip         ip地址
    id         id号
    user        用户名
    password    密码
    self.conInstrument   连接仪器套接字对象
    Connect()   连接仪器函数
    Status()    采集状态
    RealTimeData()  采集实时数据
    FiveMinuteData()采集5分钟数据
    StopData()  终止实时数据
    TodayData() 采集当天数据
'''

import socket
from icmp_ping import *
import sqlite3
import time
import matplotlib.pyplot as plt
import numpy as np
import io
from DB.database import Database


import threading

lock = threading.Lock()


class DataCollection:
    '数据采集、信息采集'

    # info为仪器状态信息列表，info=[ip,id,username,password,instrproject,instrtype]
    def __init__(self, info):
        self.info = info
        self.conInstrument = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 套接字对象，通过socket
        self.conInstrument.settimeout(100)
        self.o_network = 0
        self.o_connect = 0
        self.o_login = 0
        self.o_yqstatus = 0
        self.o_realdata = 0
        self.o_todaydata = 0
        self.o_fivedata = 0
        self.o_stop = 0

    def Network(self):
        print('开始测试%s网络' % self.info[0])
        d = ping(self.info[0].strip(), timeout=2, count=2)
        if d == 0:
            o_network = 0
            print('网络正常')
        else:
            o_network = 1
            print('网络异常')


    # 连接仪器
    def Connect(self):
        '''
        正常：$ack\n   不正常：$nak\n  仪器收到错误指令：$err\n $err\r\n
        '''
        print("开始测试%s仪器连接"%self.info[0])
        try:
            self.conInstrument.connect((self.info[0].strip(), 81))
            self.o_connect = 0
            print('连接正常')
        except socket.timeout:
            print('无法连接')
            self.o_connect = 1
        except TimeoutError:
            print('无法连接')
            self.o_connect = 1

    #登录仪器
    def Login(self):
        print("开始测试%s仪器登录"%self.info[5])
        order = "get /42+{0}+lin+{1}+{2} /http/1.1".format(self.info[1], self.info[4], self.info[5])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem = self.conInstrument.recv(100)
            print(tem)
            if b'ack\n' in tem:
                self.o_login = 0
                print('登录正常')
            elif tem == b'$nak\n':
                self.o_login = 1
            elif tem == b'$err\n':
                self.o_login = 2
            else:
                self.o_login = 4
        except socket.timeout:
            self.o_login = 3

    #判断接收的数据是否正常
    def Conversion_status(self, oneData):
        try:
            data = oneData.split('\n')[1].split(' ')
            shijiLength = len(data)
            print(shijiLength)
            if shijiLength >= 10:
                diffTime = round(time.time()-time.mktime(time.strptime(data[1],"%Y%m%d%H%M%S")))
                return ( time.strftime("%Y%m%d%H%M%S", time.localtime()),  data[1], diffTime, data[2], data[4], data[5], data[8], data[9])
            else:
                diffTime = round(time.time() - time.mktime(time.strptime(data[1], "%Y%m%d%H%M%S")))
                return  ( time.strftime("%Y%m%d%H%M%S", time.localtime()),  data[1], diffTime, data[2], data[4], data[5], None, None)
        except:
            print("状态数据出错", oneData)

    # 采集状态信息, 36 20191222214137 2 0 0 0 0 0 0 0 00 取1,2,4,5,8,9
    def Status(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        print("开始测试仪器状态", self.info[0])
        order = "get /19+{0}+ste /http/1.1".format(self.info[1])
        # 测试仪器状态
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem_status = self.conInstrument.recv(100)
            if b'ack\n' in tem_status:
                self.o_yqstatus = 0
                return self.Conversion_status(tem_status.decode())
            elif tem_status == b'$nak\n':
                self.o_yqstatus = 1
                return None
            elif tem_status == b'$err\n':
                self.o_yqstatus = 2
                return None
            else:
                self.o_yqstatus = 4
                return None
        except socket.timeout:
            self.o_yqstatus = 3
            return None

    # 实时采集数据
    def RealTimeData(self):
        print("开始测试仪器实时数据")
        order = "get /21+{0}+dat+0 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        while True:
            try:
                tem_real = self.conInstrument.recv(300)
                if b'ack\n' in tem_real:
                    self.o_realdata = 0
                    dataContent = tem_real.decode().split('\n')[1].split(' ')  # 数据包的主内容
                    print(dataContent)
                    dataTime = dataContent[1]
                    dataSample = dataContent[4]
                    dataLength = int(dataContent[5])
                    data = [dataTime,dataContent[-dataLength:]]
                    realdataTab[self.info[1]]['data'].append(data)
                    lock.acquire()
                    realdataTab['data'] = dataContent
                    lock.release()
                    # 停止实时数据
                    #self.StopData()
                elif tem_real == b'$nak\n':
                    self.o_realdata = 1
                    print(tem_real)
                elif tem_real == b'$err\n':
                    self.o_realdata = 2
                    print(tem_real)
                else:
                    self.o_realdata = 4
                    print(tem_real)
            except socket.timeout:
                self.o_realdata = 3

    # 实时采集数据（一个）
    def RealTimeOneData(self):
        print("开始测试仪器实时数据")
        order = "get /21+{0}+dat+0 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem_real = self.conInstrument.recv(300)
            if b'ack\n' in tem_real:
                self.o_realdata = 0
                print(tem_real)

                # 停止实时数据
                #self.StopData()
            elif tem_real == b'$nak\n':
                self.o_realdata = 1
                print(tem_real)
            elif tem_real == b'$err\n':
                self.o_realdata = 2
                print(tem_real)
            else:
                self.o_realdata = 4
                print(tem_real)
        except socket.timeout:
            self.o_realdata = 3


    # 采集5分钟数据
    def FiveData(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        print("开始测试仪器五分钟数据")
        order = "get /21+{0}+dat+5 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem_fivedata = self.conInstrument.recv(500)
            if b'ack\n' in tem_fivedata:
                self.o_fivedata = 0
                print(tem_fivedata)

            elif tem_fivedata == '$nak\n':
                self.o_fivedata = 1
            elif tem_fivedata == '$err\n':
                self.o_fivedata = 2
            else:
                self.o_fivedata = 4
        except socket.timeout:
            self.o_fivedata = 3

    # 终止实时数据
    def StopData(self):
        print('终止实时数据')
        order = "get /19+{0}+stp /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem = self.conInstrument.recv(200)
            if b'ack\n' in tem:
                self.o_stop = 0
                print('终止实时数据成功')
            elif tem == '$nak\n':
                self.o_stop = 1
            elif tem == '$err\n':
                self.o_stop = 2
            else:
                self.o_stop = 4
        except socket.timeout:
            self.o_stop = 3


    #关闭套接字
    def Close(self):
        self.conInstrument.close()

    # 采集当天数据
    def TodayData(self):
        # 测试当天数据
        print("开始测试仪器当天数据")
        order = "get /23+{0}+dat+1+0 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        tem_todaydata = []
        while True:
            try:
                data = self.conInstrument.recv(50000)
                if not data: break
                tem_todaydata.append(data)
                tem1 = b''.join(tem_todaydata)
                if b'ack\n' in tem1:
                    print(tem1)
                    self.o_todaydata = 0
                    break
                elif tem1 == '$nak\n':
                    self.o_todaydata = 1
                elif tem1 == '$err\n':
                    self.o_todaydata = 2
                else:
                    self.o_todaydata = 4
            except socket.timeout:
                print("采集当天数据阻塞")
                self.o_todaydata = 3
                break


def Main1():
    conn = sqlite3.connect('../web_oct/yq.db')
    c = conn.cursor()
    yqinfo = list(c.execute("SELECT INSTRIP,INSTRID,USERNAME,PASSWORD,INSTRPROJECT,INSTRTYPE FROM CAPACITY "))
    for i in yqinfo:
        dc = DataCollection(i)
        dc.Network()
        if dc.o_network == 0:
            dc.Connect()
            if dc.o_connect == 0:
                dc.Login()
                if dc.o_login == 0:
                    #dc.Status()
                    dc.FiveData()
                    #dc.TodayData()
                    #dc.RealTimeOneData()

                    print(dc.o_realdata)

def Status_dict():
    for i in yqinfo:
        dc = DataCollection(i)
        dc.Network()
        statusTab[i[1]]['network'] = dc.o_network
        if dc.o_network == 0:
            dc.Connect()
            if dc.o_connect == 0:
                dc.Login()
                if dc.o_login == 0:
                    statusTab[i[1]]['status'] = dc.Status()

def Realdata_dict(yqAlone):
    dc = DataCollection(yqAlone)
    dc.Network()
    if dc.o_network == 0:
        dc.Connect()
        if dc.o_connect == 0:
            dc.Login()
            if dc.o_login == 0:
                dc.RealTimeData()

def MainYqRealTime():
    "主函数"
    print("开始")
    # 开多进程
    starttime = time.time()
    threads = []
    nloops = range(len(yqinfo))
    for i in nloops:
        t = threading.Thread(target=Realdata_dict, args=(yqinfo[i],))
        threads.append(t)
    for i in range(len(yqinfo)):
        threads[i].start()
    endtime = time.time()
    huatime =  endtime-starttime
    print('all Done at:',huatime)

def Main():
    # 定义变量
    global attributeTab         # 属性字典
    global statusTab            # 状态字典
    global realdataTab          # 实时数据字典
    attributeTab = {}
    statusTab = {}
    realdataTab = {}

    conn = sqlite3.connect('../web_oct/yq.db')
    c = conn.cursor()
    yqinfo = list(c.execute("SELECT * FROM CAPACITY"))
    for i in yqinfo:
        attributeTab[i[1]] = {'instrip': i[0].strip(), 'stationid':i[2], 'pointid':i[3], 'username':i[4], \
                              'password':i[5], 'instrproject':i[6], 'stationname':i[7],'instrname':i[8],\
                              'instrtype':i[9], 'samplerate': i[10], 'itemnum':i[11], 'network':i[12],\
                              'connection':i[13], 'login': i[14], 'yqstatus': i[15], 'realdata':i[16],\
                              'todaydata':i[17], 'fivedata':i[18]}
        statusTab[i[1]] = {'network': None, 'status': None}
        realdataTab[i[1]] = {'lastTime': None, 'length':0, 'data': []}

if __name__ == "__main__":

    Main()
    print(attributeTab)
    """
    yqinfo = list(c.execute("SELECT INSTRIP,INSTRID,USERNAME,PASSWORD,INSTRPROJECT,INSTRTYPE FROM CAPACITY WHERE REALDATA=0"))

    for i in yqinfo:
        statusTab[i[1]] = {'network': None, 'status': None}
        realdataTab[i[1]] = {'lastTime':None, 'data':[]}
    print("开始")
    """
    """
    #Status_dict()
    for k,v in statusTab.items():
        if v['status'] is not None:
            print(v['status'][2])
        else:
            print('无')
    """
    #Main()

    #MainYqRealTime()
    print("结束")