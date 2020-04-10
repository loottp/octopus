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
import re


import threading
import logging
import traceback

import queue
import threading
import requests


exitFlag = 0

class myThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        print ("开启线程：" + self.name)
        process_data(self.name, self.q)
        print ("退出线程：" + self.name)

def process_data(threadName, q):
    while True:
        queueLock.acquire()
        if not workQueue.empty():
            data = q.get()
            queueLock.release()
            dc = DataCollection(data)
            if statusTab[data[1]]['network'] == 0:
                dc.Connect()
                if dc.o_connect == 0:
                    statusTab[data[1]]['connection'] = 0
                    dc.Login()
                    if dc.o_login == 0:
                        statusTab[data[1]]['login'] = 0
                        statusTab[data[1]]['status'] = dc.Status()
                        if data[16] == 1 and data[17] == 0:
                            dc.FiveData()
                        dc.Close()
                    else:
                        statusTab[data[1]]['login'] = 1
                        dc.Close()
                else:
                    statusTab[data[1]]['connection'] = 1
                    dc.Close()
            else:
                statusTab[data[1]]['network'] = 1
                dc.Close()
        else:
            queueLock.release()
        time.sleep(1)
workQueue = queue.Queue(100)
queueLock = threading.Lock()


lock = threading.Lock()
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(threadName)s - %(thread)d - %(message)s"
logging.basicConfig(filename='log.log', format=LOG_FORMAT)

class DataCollection:
    '数据采集、信息采集'

    # info为仪器状态信息列表，info=[ip,id,username,password,instrproject,instrtype]
    def __init__(self, info):
        self.info = info
        self.conInstrument = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 套接字对象，通过socket
        self.conInstrument.settimeout(10)
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
        print('IP:%s,d的值为%d'%(self.info[0], d))
        if d == 0:
            self.o_network = 0
        else:
            self.o_network = 1
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
        except socket.timeout:
            print('无法连接')
            self.o_connect = 1
        except TimeoutError:
            print('无法连接')
            self.o_connect = 1
        except:
            self.o_connect = 1
            # 报警
            msg = '发生无法预料的异常，连接遭到拒绝！！！'
            self.Warning(msg)
            self.Close()

    #登录仪器
    def Login(self):
        print("开始测试%s仪器登录"%self.info[0])
        order = "get /{0}+{1}+lin+{2}+{3} /http/1.1".format(str(21 + len(self.info[4])+len(self.info[5])), self.info[1], self.info[4], self.info[5])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem = self.conInstrument.recv(100)
            if b'ack\n' in tem:
                self.o_login = 0
            elif tem == b'$nak\n':
                self.o_login = 1
            elif tem == b'$err\n':
                self.o_login = 2
            else:
                self.o_login = 4
        except socket.timeout:
            self.o_login = 3
        except:
            self.o_login = 3
            msg = '发生无法预料的异常，无法登录！！！'
            self.Warning(msg)
            self.Close()

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
        except:
            self.o_yqstatus = 3
            return None


    # 实时数据导入到字典中
    def RealdataImport(self, data):
        realdataTab[self.info[1]]['data'].insert(0, data)
        realdataTab[self.info[1]]['length'] = realdataTab[self.info[1]]['length'] + 1
        if realdataTab[self.info[1]]['length'] > 5:
            realdataTab[self.info[1]]['data'].pop()
            realdataTab[self.info[1]]['length'] = realdataTab[self.info[1]]['length'] - 1

    # 实时采集数据
    def RealTimeData(self):
        self.conInstrument.settimeout(100)
        print(threading.currentThread().ident)
        print("开始测试仪器实时数据")
        order = "get /21+{0}+dat+0 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        itemnum = attributeTab[self.info[1]]['itemnum']
        print(itemnum)
        noneLength = 0
        tem_real = b''
        while True:
            try:
                tem_real += self.conInstrument.recv(300)
                content_ack = re.match(b'\$(.*?)ack\n', tem_real, re.DOTALL)

                if content_ack is not None:
                    tem_real = tem_real[content_ack.span()[1]:]
                    noneLength = 0
                    statusTab[self.info[1]]['importData'] = 0
                    self.o_realdata = 0
                    dataContent = content_ack.group().decode().split('\n')[1].split(' ')  # 数据包的主内容
                    dataTime = dataContent[1]
                    data = [dataTime,dataContent[-itemnum:]]
                    #lock.acquire()
                    # 把数据导入到字典中
                    self.RealdataImport(data)
                    #lock.release()
                else:
                    content_nak = re.match(b'\$nak\n', tem_real, re.DOTALL)
                    if content_nak is not None:
                        print(tem_real)
                        tem_real = tem_real[content_nak.span()[1]:]
                        self.o_realdata = 1
                        statusTab[self.info[1]]['importData'] = 1
                    else:
                        content_err = re.match(b'\$err\n', tem_real, re.DOTALL)
                        if content_err is not None:
                            print(tem_real)
                            tem_real = tem_real[content_err.span()[1]:]
                            self.o_realdata = 2
                            statusTab[self.info[1]]['importData'] = 1

                        elif tem_real == b'':
                            noneLength += 1
                            if noneLength > 100:
                                statusTab[self.info[1]]['importData'] = 1
                                # 报警
                                msg = '采集实时数据时变量tem_reals收到空数据大于300'
                                self.Warning(msg)
                                self.Close()

                                break

                        elif len(tem_real)>600:
                            # 报警
                            msg = '采集实时数据出现异常,变量tem_reals数据大于600' + tem_real.decode()
                            self.Warning(msg)
                            self.Close()

                            break

            except socket.timeout:
                self.o_realdata = 3
                statusTab[self.info[1]]['importData'] = 1
                # 报警
                msg = '采集实时数据超时，发生了阻塞'
                self.Warning(msg)
                self.Close()

                break
            except:
                s = traceback.format_exc()
                logging.error(s)
                # 报警
                msg = '出现异常,变量tem_real:'+tem_real.decode()
                self.Warning(msg)
                self.Close()

                break



    # 输出报警信息并关闭套接字
    def Warning(self, msg):
        message = self.info[0] + self.info[1] + self.info[2] + self.info[3] + self.info[7] + msg
        logging.warning(message)


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

    # 采集5分钟数据,实验用
    def FiveData_1(self):
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

    # 采集5分钟数据
    def FiveData(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        print("开始测试仪器五分钟数据")
        itemnum = attributeTab[self.info[1]]['itemnum']
        order = "get /21+{0}+dat+5 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem_fivedata = self.conInstrument.recv(500)
            if b'ack\n' in tem_fivedata:
                self.o_fivedata = 0
                dataContent = tem_fivedata.decode().split('\n')[1].split(' ')  # 数据包的主内容
                dataTime = dataContent[1]
                data = [dataTime, dataContent[-itemnum-1:-1]]

                # 把数据导入到字典中，使用内部函数
                self.RealdataImport(data)


            elif tem_fivedata == b'$nak\n':
                self.o_fivedata = 1

                # 报警
                msg = '变量tem_fivedata收到数据nak'
                self.Warning(msg)
                self.Close()
            elif tem_fivedata == b'$err\n':
                self.o_fivedata = 2

                # 报警
                msg = '变量tem_fivedata收到数据err'
                self.Warning(msg)
                self.Close()
            else:
                self.o_fivedata = 4

                # 报警
                msg = '变量tem_fivedata收到无法解析的数据'
                self.Warning(msg)
                self.Close()
        except socket.timeout:
            self.o_fivedata = 3

            # 报警
            msg = '采集五分钟数据超时，发生了阻塞'
            self.Warning(msg)
            self.Close()
        except:
            self.o_fivedata = 3
            # 报警
            msg = '采集五分钟数据发生了无法预料的异常'
            self.Warning(msg)
            self.Close()

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
        except:
            self.o_stop = 3
            # 报警
            msg = '停止数据发送发生了无法预料的异常'
            self.Warning(msg)
            self.Close()



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

#九五数据转换
def Conversion_JW_np(data):
    try:
        if data == 'AAAAAA':
            return None
        elif data[0] == 'A':
            return str(-float(data[1:])/10)
        elif data[0] == '2':
            return str(float(data[1:])/10)
        else:
            return None
    except:
        print('无法转换')
        return None

# 采集南平台九五
def JW_np():
    while True:
        c = requests.get('http://10.35.185.111/recvcurrent1').content.decode()
        data = re.findall(r"\'(.*?)\'", c)
        if len(data) == 3:
            data = list(map(Conversion_JW_np, data))
            # 垂直摆倾斜仪数据
            realdataTab['J222DQYQ9548']['data'].insert(0, data[0:2])
            realdataTab['J222DQYQ9548']['length'] = realdataTab['J222DQYQ9548']['length'] + 1
            if realdataTab['J222DQYQ9548']['length'] > 5:
                realdataTab['J222DQYQ9548']['data'].pop()
                realdataTab['J222DQYQ9548']['length'] = realdataTab['J222DQYQ9548']['length'] - 1

            # 硐温仪数据
            realdataTab['J231DQYQ9548']['data'].insert(0, str(float(data[2])/10))
            realdataTab['J231DQYQ9548']['length'] = realdataTab['J231DQYQ9548']['length'] + 1
            if realdataTab['J231DQYQ9548']['length'] > 5:
                realdataTab['J231DQYQ9548']['data'].pop()
                realdataTab['J231DQYQ9548']['length'] = realdataTab['J231DQYQ9548']['length'] - 1
        else:
            print('无法采集到数据')
        time.sleep(3593)

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


def Realdata_dict(yqOne):
    while True:
        print('开始实时数据采集')
        dc = DataCollection(yqOne)
        if statusTab[yqOne[1]]['network'] == 0:
            dc.Connect()
            if dc.o_connect == 0:
                statusTab[yqOne[1]]['connection'] = 0
                dc.Login()
                if dc.o_login == 0:
                    statusTab[yqOne[1]]['login'] = 0
                    dc.RealTimeData()
        print('实时数据采集意外停止，暂停5分钟')
        time.sleep(300)

def Status_dict(yqinfo):
    threadList = ["Thread-1", "Thread-2", "Thread-3", "Thread-4", "Thread-5", "Thread-6", "Thread-7", "Thread-8", "Thread-9", "Thread-10", "Thread-11", "Thread-12"]
    threadID = 1
    threads1 = []
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue)
        thread.start()
        threads1.append(thread)
        threadID += 1
    while True:
        queueLock.acquire()
        for i in yqinfo:
            workQueue.put(i)
        queueLock.release()
        """
        startTime = time.time()
        while not workQueue.empty():
            pass
        endTime = time.time()
        print("本次用时：", endTime - startTime)
        """
        time.sleep(3600)

def Network(yqinfo):
    while True:
        try:
            for i in yqinfo:
                #print('开始测试%s网络' % i[0])
                d = ping(i[0].strip(), timeout=2, count=2)
                if d == 0:
                    statusTab[i[1]]['network'] = 0
                else:
                    statusTab[i[1]]['network'] = 1
                    #print('%s网络异常'%i[0])
            time.sleep(20)
        except:
            print("ping")

def Watchdog(yqinfo, si):
    while True:
        for i in si:
            abnormalTab['networks'][i[0]]['abn_instr_nums'] = 0
            abnormalTab['networks'][i[0]]['stationnetwork'] = 0
            abnormalTab['networks'][i[0]]['abngateway'] = 0
        for i in yqinfo:
            # 扫描网络
            if statusTab[i[1]]['network'] == 1:
                abnormalTab['networks'][i[2]]['abn_instr_nums'] += 1
                abnormalTab['networks'][i[2]]['abngateway'] = ping(abnormalTab['networks'][i[2]]['instrgateway'], count=4, timeout=2)
                if abnormalTab['networks'][i[2]]['abn_instr_nums'] == abnormalTab['networks'][i[2]]['instrnums']:
                    abnormalTab['networks'][i[2]]['stationnetwork'] = 1
        time.sleep(60)


def Main():
    # 定义变量
    global attributeTab         # 属性字典
    global statusTab            # 状态字典
    global realdataTab          # 实时数据字典
    global abnormalTab          # 异常字典
    attributeTab = {}
    statusTab = {}
    realdataTab = {}
    threads = []
    abnormalTab = {'networks':{}, 'yiqi':{}}

    conn = sqlite3.connect('../web_oct/yq.db')
    c = conn.cursor()
    yqinfo = list(c.execute("SELECT * FROM CAPACITY WHERE NETWORK=0  "))
    si = list(c.execute("SELECT * FROM SI"))
    for i in si:
        abnormalTab['networks'][i[0]] = {'stationname':i[1], 'instrnums':i[2], 'instrgateway':i[3], 'abn_instr_nums':0, 'abngateway':0, 'stationnetwork':0}

    for i in yqinfo:
        attributeTab[i[1]] = {'instrip': i[0].strip(), 'stationid':i[2], 'pointid':i[3], 'username':i[4], \
                              'password':i[5], 'instrproject':i[6], 'stationname':i[7],'instrname':i[8],\
                              'instrtype':i[9], 'samplerate': i[10], 'itemnum':i[11], 'network':i[12],\
                              'connection':i[13], 'login': i[14], 'yqstatus': i[15], 'realdata':i[16],\
                              'todaydata':i[17], 'fivedata':i[18]}
        statusTab[i[1]] = {'network': 0, 'status': None, 'connection':None, 'login':None, 'importData':None}
        realdataTab[i[1]] = {'lastTime': None, 'length':0, 'data': []}


    # PING网络
    t1 = threading.Thread(target=Network, args=(yqinfo, ))
    threads.append(t1)

    # 采集状态、部分五分钟数据
    t2 = threading.Thread(target=Status_dict, args=(yqinfo,))
    threads.append(t2)

    #采集实时数据
    for i in yqinfo:
        if i[16] == 0:
            t = threading.Thread(target=Realdata_dict, args=(i,))
            threads.append(t)
    
    #采集南平九五数据
    t3 = threading.Thread(target=JW_np)
    threads.append(t3)

    #看门狗
    t4 = threading.Thread(target=Watchdog, args=(yqinfo, si))
    threads.append((t4))

    for i in threads:
        i.start()



if __name__ == "__main__":
    print('开始1')
    Main()
    print("结束")