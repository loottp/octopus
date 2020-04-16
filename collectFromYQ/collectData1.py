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
import time
import re


import threading
import logging
import traceback

import queue
import threading
import requests

class myThread (threading.Thread):
    def __init__(self, q):
        threading.Thread.__init__(self)
        self.q = q
    def run(self):
        print('线程启动')
        process_data(self.q)

def process_data(q):
    while True:
        try:
            queueLock.acquire()
            if not workQueue.empty():
                info = q.get()
                queueLock.release()
                dc = DataCollection(info)
                if statusTab[info[1]]['network'] == 0:
                    dc.Connect()
                    if dc.o_connect == 0:
                        statusTab[info[1]]['connection'] = 0
                        dc.Login()
                        if dc.o_login == 0:

                            statusTab[info[1]]['login'] = 0
                            onedata = dc.Status()
                            statusTab[info[1]]['status'] = conversion_status(onedata)
                            if info[16] == 1 and info[18] == 0:
                                onedata = dc.FiveData()
                                print(onedata)
                                onedata1 = conversion_fivedata(info, onedata)
                                realdataImport(info[1], onedata1)
                            dc.Close()

                        else:
                            statusTab[info[1]]['login'] = 1
                            dc.Close()
                    else:
                        statusTab[info[1]]['connection'] = 1
                        dc.Close()
                else:
                    statusTab[info[1]]['network'] = 1
                    dc.Close()
            else:
                queueLock.release()
            time.sleep(1)
        except:
            print('process_data发生异常')
            time.sleep(1)


workQueue = queue.Queue(100)
queueLock = threading.Lock()


# lock = threading.Lock()
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(threadName)s - %(thread)d - %(message)s"
logging.basicConfig(filename='log.log', format=LOG_FORMAT)

# 字典
attributeTab = {}       # 属性字典
statusTab = {}          # 状态字典
realdataTab = {}        # 实时数据字典
threads = []
abnormalTab = {'networks':{}, 'yiqi':{}}        # 异常字典

class DataCollection:
    '数据采集、信息采集'

    # info为仪器状态信息列表，info=[ip,id,username,password,instrproject,instrtype]
    def __init__(self, info, bq):
        self.info = info
        self.conInstrument = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 套接字对象，通过socket
        self.conInstrument.settimeout(10)
        self.o_connect = 0
        self.o_login = 0
        self.o_yqstatus = 0
        self.o_realdata = 0
        self.o_todaydata = 0
        self.o_fivedata = 0
        self.o_stop = 0
        self.bq = bq

    # 采集工作参数
    def WorkParameters(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        print("开始采集仪器工作参数", self.info[0:4],self.info[7:10])
        order = "get /21+{0}+pmr+m /http/1.1".format(self.info[1])
        # 测试仪器状态
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem_parameters = self.conInstrument.recv(200)
            print(tem_parameters)
            if b'ack\n' in tem_parameters:
                self.o_yqstatus = 0
                print(tem_parameters)
                dataContent = tem_parameters.decode().split('\n')[1].split(' ')
                attributeTab[self.info[1]]['working_parameterNum'] = int(dataContent[3])
                attributeTab[self.info[1]]['working_parameter'] = dataContent[4:]

            elif tem_parameters == b'$nak\n':
                self.o_yqstatus = 1
                return None
            elif tem_parameters == b'$err\n':
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

    # 连接仪器
    def Connect(self, bq):
        '''
        正常：$ack\n   不正常：$nak\n  仪器收到错误指令：$err\n $err\r\n
        '''
        print("开始测试%s仪器连接"%self.info[0])
        try:
            self.conInstrument.connect((self.info[0].strip(), 81))
            self.o_connect = 0
        except socket.timeout:
            c_time = int(time.time())
            bq.put([0, 1, self.info[1], c_time, 'real+1'])
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

    # 采集状态信息, 36 20191222214137 2 0 0 0 0 0 0 0 00 取1,2,4,5,8,9
    def Status(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        print("开始测试仪器状态", self.info[0])
        order = "get /19+{0}+ste /http/1.1".format(self.info[1])
        # 测试仪器状态
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem_status = self.conInstrument.recv(100)
            #print(tem_status)
            if b'ack\n' in tem_status:
                self.o_yqstatus = 0
                return tem_status.decode()
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

    # 实时采集数据
    def RealTimeData(self, bq):
        self.conInstrument.settimeout(100)
        print(threading.currentThread().ident)
        print("开始测试仪器实时数据")
        order = "get /21+{0}+dat+0 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        tem_real = b''
        noneLength = 0
        while True:
            try:
                tem_real += self.conInstrument.recv(300)
                content_ack = re.match(b'\$(.*?)ack\n', tem_real, re.DOTALL)  # 加re.DOTALL代表匹配所有字符包括\n
                c_time = int(time.time())
                if content_ack is not None:
                    tem_real = tem_real[content_ack.span()[1]:]  # 把剩下的还给tem_real
                    datatem = content_ack.group().decode().split('\n')[1].split(' ')
                    itemnum = int(datatem[5])
                    data = datatem[-itemnum:]
                    bq.put([0, 0, self.info[1], c_time, data])
                    self.o_realdata = 0
                    noneLength = 0
                else:
                    content_nak = re.match(b'\$nak\n', tem_real, re.DOTALL)
                    if content_nak is not None:
                        bq.put([0, 1, self.info[1], c_time, 'real+1'])
                        msg = '实时数据收到nak'
                        Warning(self.info, msg)
                        self.Close()
                        break
                    else:
                        content_err = re.match(b'\$err\n', tem_real, re.DOTALL)
                        if content_err is not None:
                            bq.put([0, 1, self.info[1], c_time, 'real+2'])
                            msg = '实时数据收到err'
                            Warning(self.info, msg)
                            self.Close()

                        elif tem_real == b'':
                            noneLength += 1
                            if noneLength > 100:
                                bq.put([0, 1, self.info[1], c_time, 'real+4'])
                                # 报警
                                msg = '采集实时数据时变量tem_reals收到空数据大于300，仪器端已经断开'
                                Warning(self.info, msg)
                                self.Close()
                                break

                        elif len(tem_real) > 600:
                            bq.put([0, 1, self.info[1], c_time, 'real+5'])
                            # 报警
                            msg = '采集实时数据出现异常,变量tem_reals数据大于600' + tem_real.decode()
                            Warning(self.info, msg)
                            self.Close()

                            break

            except socket.timeout:
                c_time = int(time.time())
                bq.put([0, 1, self.info[1], c_time, 'real+3'])
                # 报警
                msg = '采集实时数据超时，发生了阻塞'
                Warning(self.info, msg)
                self.Close()
                break
            except:
                c_time = int(time.time())
                bq.put([0, 1, self.info[1], c_time, 'real+6'])
                s = traceback.format_exc()
                logging.error(s)
                # 报警
                msg = '出现异常,变量tem_real:' + tem_real.decode()
                Warning(self.info, msg)
                self.Close()
                break


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
                return tem_fivedata.decode()

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

    # 关闭套接字
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
                    self.o_todaydata = 0
                    return tem1.decode()
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

    # 发送数据
    def send(self, order):
        self.conInstrument.sendall(order)

    # 收取数据
    def recv(self):
        try:
            tem_status = self.conInstrument.recv(100)
            #print(tem_status)
            if b'ack\n' in tem_status:
                self.o_yqstatus = 0
                return tem_status.decode()
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
def order(stagescode, info):
    if stagescode == 'con':
        return bytes("get /19+{0}+ste /http/1.1".format(info[1]), encoding="utf-8")   #状态
    elif stagescode == 'login':
        return bytes("get /{0}+{1}+lin+{2}+{3} /http/1.1".\
                     format(str(21 + len(info[4]) + len(info[5])), info[1], info[4], info[5]), encoding="utf-8")#login
    elif stagescode == 'real':
        return bytes("get /21+{0}+dat+0 /http/1.1".format(info[1]), encoding="utf-8")#实时
    elif stagescode == 'five':
        return bytes("get /21+{0}+dat+5 /http/1.1".format(info[1]), encoding="utf-8")#五分钟
    elif stagescode == 'param':
        return bytes("get /21+{0}+pmr+m /http/1.1".format(info[1]), encoding="utf-8") #参数
    elif stagescode == 'stop':
        return bytes("get /19+{0}+stp /http/1.1".format(info[1]), encoding="utf-8")#stop实时
    elif stagescode == 'today':
        return bytes("get /23+{0}+dat+1+0 /http/1.1".format(info[1]), encoding="utf-8")#当天


# 输出报警信息并关闭套接字
def Warning(info, msg):
    message = info[0] + info[1] + info[2] + info[3] + info[7] + msg
    logging.warning(message)

# 五分钟数据导入到实时数据字典中
def realdataImport(id, data):
    # 描述： 把实时数据导入到realdataTab字典中
    #
    # 输入：仪器信息， 实时数据包（从conversion_realdata函数中返回
    #
    # 输出：把数据直接导入到字典中
    #
    realdataTab[id]['data'].insert(0, data)
    realdataTab[id]['length'] = realdataTab[id]['length'] + 1
    if realdataTab[id]['length'] > 5:
        realdataTab[id]['data'].pop()
        realdataTab[id]['length'] = realdataTab[id]['length'] - 1

# 清洗五分钟数据包
def conversion_fivedata(info, onedata):
    # 描述： 对fivedata数据包进行清洗转换
    #
    # 输入：info（仪器信息），onedata（五分钟数据包）
    #
    # 输出：[数据包时间， [data1, data2, ....] ]
    #       其中data8，data9如果没有，输出为None
    itemnum = attributeTab[info[1]]['itemnum']
    dataContent = onedata.split('\n')[1].split(' ')  # 数据包的主内容
    dataTime = int(time.time())
    return [dataTime, dataContent[-itemnum-1:-1]]


# 清洗转换status数据包
def conversion_status(onedata):
    # 描述： 对status数据包进行清洗转换
    #
    # 输入：'$36\n36 20200414085126 2 0 0 0 0 0 0 0 00\nack\n'
    #
    # 输出：[本地时间，数据包时间， 钟差， data2，data4， data5， data8， data9 ]
    #       其中data8，data9如果没有，输出为None
    try:
        data = onedata.split('\n')[1].split(' ')
        shijiLength = len(data)
        #print(shijiLength)
        if shijiLength >= 10:
            diffTime = round(time.time()-time.mktime(time.strptime(data[1],"%Y%m%d%H%M%S")))
            return [time.strftime("%Y%m%d%H%M%S", time.localtime()),  data[1], diffTime, data[2], data[4], data[5], data[8], data[9]]
        else:
            diffTime = round(time.time() - time.mktime(time.strptime(data[1], "%Y%m%d%H%M%S")))
            return [time.strftime("%Y%m%d%H%M%S", time.localtime()),  data[1], diffTime, data[2], data[4], data[5], None, None]
    except:
        print("状态数据出错", onedata)

#九五数据转换
def Conversion_JW_np(data):
    # 描述： 把采集于网页的数据进行转换
    #
    # 输入：单个数据，如'A012345'等字符串
    #
    # 输出：单个数据，转换成'-1234.5'
    #
    try:
        if data == 'AAAAAA':
            return 'NULL'
        elif data[0] == 'A':
            return str(-float(data[1:])/10)
        elif data[0] == '2':
            return str(float(data[1:])/10)
        else:
            return 'NULL'
    except:
        print('无法转换')
        return 'NULL'

# 采集南平台九五
def JW_np():
    # 描述： 专用采集南平台协转，有两套仪器，VS垂直摆和硐温仪，3个分量
    #
    # 输入：无
    #
    # 输出：无，直接导入到字典中，
    #
    # vs垂直摆[数据包时间， [data1, data2] ]，硐温仪[数据包时间， [data1] ]
    #
    while True:
        c = requests.get('http://10.35.185.111/recvcurrent1').content.decode()
        data = re.findall(r"\'(.*?)\'", c)
        if len(data) == 3:
            data1 = list(map(Conversion_JW_np, data))
            # 垂直摆倾斜仪数据
            realdataImport('J222DQYQ9548', [int(time.time()), data1[0:2]])

            # 硐温仪数据, 转换的数据缺少0.1倍数。
            if data1[2] != 'NULL':
                realdataImport('J231DQYQ9548', [int(time.time()), [str(float(data1[2])/10)]])
            else:
                realdataImport('J231DQYQ9548', [int(time.time()), [data1[2]]])
        else:
            print('无法采集到数据')
        time.sleep(3593)

#
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
        print('实时数据采集无网络或，意外停止，暂停5分钟')
        time.sleep(300)

def Status_dict(yqinfo):
    # 描述： 开多个线程，用于采集status数据包、部分五分钟数据采集
    #       多个线程共用一个队列，从队列中取数
    # 输入：yqinfo（所有的仪器信息）
    #
    # 输出：无
    #

    # 线程数由yqinfo数量决定，最多10个线程。
    if len(yqinfo) > 10:
        threads_num = 10
    else:
        threads_num = len(yqinfo)

    # threads1为线程列表
    threads1 = []
    for i in range(threads_num):
        thread = myThread(workQueue)
        thread.start()
        threads1.append(thread)

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
    # 描述： 采集网络连通性
    #
    # 输入：yqinfo（所有的仪器信息）
    #
    # 输出：无，数据直接存入到statusTab字典中
    #
    while True:
        for i in yqinfo:
            try:
                d = ping(i[0].strip(), timeout=2, count=2)
                if d == 0:
                    statusTab[i[1]]['network'] = 0
                else:
                    statusTab[i[1]]['network'] = 1
            except:
                print("ping中出现异常", i)
        time.sleep(20)

def Watchdog(yqinfo, si):
    # 描述： 监控statusTab、realdataTab
    #
    # 输入：yqinfo（所有的仪器信息）
    #
    # 输出：无，数据直接存入到statusTab字典中
    #
    k = None
    while True:
        try:
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

            # 扫描当前值有无超量程
            for i in yqinfo:
                k = i
                if attributeTab[i[1]]['working_parameter_kg'] == 0:
                    a = attributeTab[i[1]]['threshold']
                    #a = "-200 200 -200 200 -200 200 NULL NULL"
                    b = attributeTab[i[1]]['items']
                    c = attributeTab[i[1]]['working_parameter'].split(' ')
                    data = realdataTab[i[1]]['data'][0][1]
                    for i in range(len(data)):
                        data1 = (float(data[i]) - float(c[2*i+1])) / float(c[2*i])
                        print(data1)
                        if a.split(' ')[2 * i] != 'NULL':
                            if data1 < float(a.split(' ')[2 * i]):
                                print(b.split(' ')[i], "  低于下限data:",data1,  a.split(' ')[2 * i])
                                abnormalTab['yiqi'][i[1]]['overrange'].append( [int(time.time()), b.split(' ')[i]])
                        if a.split(' ')[2 * i + 1] != 'NULL':

                            if data1 > float(a.split(' ')[2 * i + 1]):
                                print(b.split(' ')[i], "  高于上限data:",data1,  a.split(' ')[2 * i + 1])
                                abnormalTab['yiqi'][i[1]]['overrange'].append([int(time.time()), b.split(' ')[i]])
        except:
            print('发生异常', k)
            time.sleep(10)
            continue
        else:
            time.sleep(60)


if __name__ == "__main__":
    print('开始1')
    #Main()
    print("结束")