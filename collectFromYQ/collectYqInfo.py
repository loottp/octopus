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
import time
import matplotlib.pyplot as plt
import numpy as np
import io
from DB.database import Database


import threading

lock = threading.Lock()


class DataCollection:
    '数据采集、信息采集'

    def __init__(self, ip, id, user, password):
        self.ip = ip
        self.id = id
        self.user = user
        self.password = password
        self.conInstrument = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 套接字对象，通过socket
        self.conInstrument.settimeout(30)
        self.conStatus = 1      #连接状态码1为连接正常 , 2为登陆正常，3为连接不上， 4为连接不正常nak，5为收到错误指令err
        self.recvStatus = 1     #接收数据状态码1为正常，2为无此项功能nak，3为错误代码err 4超时
        self.sendStatus = 1     #发送数据状态码1为正常，2为超时异常， 3为其他。
        self.dataFormatStatus = 1 #数据格式状态码1为正常，2为异常

    def recv_end(self, length):
        '尾标识方法，接收数据'
        total_data = b''
        while True:
            try:
                data = self.conInstrument.recv(length)
                if data == b'$nak\n':
                    self.recvStatus = 2
                    break
                elif data == b'$err\n':
                    self.recvStatus = 3
                    break
                else:
                    total_data += data
                    if len(total_data) > 1:
                        if b'ack\n' in total_data:
                            self.recvStatus = 1
                            break
                        if b'$nak\n' in data:
                            self.recvStatus = 2
                            break
                        if b'$err\n' in data:
                            self.recvStatus = 3
                            break
            except socket.timeout:
                self.recvStatus = 4
        return total_data.decode()

    def tuoke(self, length):
        tem = []
        try:
            while True:
                data = self.conInstrument.recv(length)
                if not data: return None
                tem.append(data)
                tem1 = b''.join(tem)
                if b'ack\n' in tem1:
                    break
                if b'$err' in tem1:
                    return None
        except socket.timeout:
            print(self.ip, " ", self.id, "无法接收到数据")
        else:
            todayData = str(b''.join(tem), encoding='utf-8')
            todayData = todayData.split('\n')[1].split(" ", 1)[1]  # 先分割取得第二个，再分割去掉第一个空格前的数据
            # print('abc',todayData)
            return todayData.strip()

    # 连接仪器
    def Connect(self):
        '''
        正常：$ack\n   不正常：$nak\n  仪器收到错误指令：$err\n $err\r\n
        '''
        # self.conInstrument = socket.socket()
        try:
            self.conInstrument.connect((self.ip, 81))
        except socket.timeout:
            self.conStatus = 3      #连接不上
            print(self.conStatus, self.sendStatus, self.recvStatus, self.dataFormatStatus)
        except TimeoutError:
            self.conStatus = 3  # 连接不上
            print('无法连接', self.ip)
            print(self.conStatus, self.sendStatus, self.recvStatus, self.dataFormatStatus)
            print('\n')
        else:
            self.conStatus = 1
            order = "get /42+{0}+lin+{1}+{2} /http/1.1".format(self.id, self.user, self.password)
            self.conInstrument.sendall(bytes(order, encoding="utf-8"))
            tem = self.conInstrument.recv(10)
            if tem == b'$ack\n':
                self.conStatus = 2
            elif tem == b'$nak\n':
                self.conStatus = 4
            elif tem == b'$err\n':
                self.conStatus = 5
            else:
                print('未知错误', tem)
            print("连接状态：", self.conStatus)

    def out(self, data):
        print(self.ip, self.id)
        print(data)
        print(self.conStatus, self.sendStatus, self.recvStatus, self.dataFormatStatus)
        print("--------------------------------")
        print('\n')

    #判断接收的数据是否正常
    def check(self, oneData):
        try:
            data = oneData.split('\n')[1]
            length = int(data.split(' ')[0])
            shijiLength = len(data)
            if length == shijiLength or length < shijiLength or length - shijiLength == 1 :
                return 1            #1为正常
            elif length - len(data) == 8:   #重力仪特有缺少8个字符
                return 2
            else:
                return 3            #0为出错误
        except:
            print(oneData)
            self.dataFormatStatus = 2
            return 0

    # 采集状态信息, 36 20191222214137 2 0 0 0 0 0 0 0 00 取1,2,4,5,8,9
    def Status(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        order = "get /19+{0}+ste /http/1.1".format(self.id)
        try:
            self.conInstrument.sendall(bytes(order, encoding="utf-8"))
            curTime = round(time.time())
        except socket.timeout:
            self.sendStatus = 2
        except TimeoutError:
            self.sendStatus = 2
        except:
            self.sendStatus = 2
        else:
            self.sendStatus = 1
            tem = self.recv_end(100)
            if tem == '':
                return None
            else:
                try:
                    if self.check(tem) == 1 :
                        data = tem.split('\n')[1].replace('  ', ' ').split(' ')
                        data = data[1:10]
                        data.append(curTime)
                        diff = round(time.mktime(time.strptime(data[0], "%Y%m%d%H%M%S")))-curTime
                        data.append(diff)
                        self.dataFormatStatus = 1
                        return data
                    elif self.check(tem) == 2:      #重力仪出现缺少字符
                        data = tem.split('\n')[1].strip().replace('  ', ' ').split(' ')
                        data = data[1:]
                        data.extend(['NULL', 'NULL', 'NULL'])
                        data.append(curTime)
                        diff = round(time.mktime(time.strptime(data[0], "%Y%m%d%H%M%S"))) - curTime
                        data.append(diff)
                        self.dataFormatStatus = 1
                        return data
                    elif self.check(tem) == 3:
                        self.dataFormatStatus = 2
                        print('数据出现异常', tem)
                        return None
                except:
                    self.dataFormatStatus = 2
                    print('数据格式不对')
                    return None

    # 实时采集数据
    # {(stationid,pointid):{instrip:ip, instrid:id, stationname:台站名, instrname:仪器名, instrtype:仪器型号}
    def RealTimeData(self, yqInfo, realData):
        order = "get /21+{0}+dat+0 /http/1.1".format(self.id)
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        self.conInstrument.settimeout(100)
        count = 0
        lock.acquire()
        realData[(yqInfo[2], yqInfo[3])] = {'instrip': yqInfo[0].strip(), 'instrid': yqInfo[1],
                                            'stationname': yqInfo[7], 'instrname': yqInfo[8], 'instrtype': yqInfo[9],
                                            'data': {}}
        lock.release()
        while True:
            realDataPacket = self.recv_end(200)  # 实时原始数据包
            count += 1
            localTime = time.strftime("%H:%M:%S", time.localtime())
            dataContent = realDataPacket.split('\n')[-3].split(' ')  # 数据包的主内容
            lock.acquire()
            realData[(yqInfo[2], yqInfo[3])]['sampleTime'] = dataContent[1]  # 采样时间，在数据包的第二项
            channels = int(dataContent[5])  # 数据通道数
            for i in range(channels):
                realData[(yqInfo[2], yqInfo[3])]['data'][dataContent[6 + i]] = dataContent[6 + channels + i]
            lock.release()

    # 采集5分钟数据
    def FiveMinuteData(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        order = "get /21+{0}+dat+5 /http/1.1".format(self.id)
        try:
            self.conInstrument.sendall(bytes(order, encoding="utf-8"))
            curTime = round(time.time())
        except socket.timeout:
            self.sendStatus = 2
        except TimeoutError:
            self.sendStatus = 2
        except:
            self.sendStatus = 2
        else:
            self.sendStatus = 1
            tem = self.recv_end(200)
            if tem == '':
                return None
            else:
                try:
                    if self.check(tem) == 1:
                        data = tem.split('\n')[1].replace('  ', ' ').split(' ')
                        data.append(curTime)
                        self.dataFormatStatus = 1
                        return data
                except:
                    print(data)
                '''
            tem = tem.replace('  ', ' ').split(" ")  # 有的字符串中有两个空格，replace去除两个空格
            itemLength = int(tem[4])
            result = tem[-itemLength:]
            print(result)
            if itemLength == 1:
                return result[0]
            else:
                result = ' '.join(result)
                return result'''

    # 终止实时数据
    def StopData(self):
        order = "get /19+{0}+stp /http/1.1".format(self.id)
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        tem = self.conInstrument.recv(200)
        print(tem)

    #关闭套接字
    def Close(self):
        self.conInstrument.close()

    # 采集当天数据
    def TodayData(self):
        order = "get /23+{0}+dat+1+0 /http/1.1".format(self.id)
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        todayData = self.tuoke(2048)
        print(todayData)
        curDate = time.strftime("%Y%m%d", time.localtime())

        filename = "35010{0}{1}.epms".format(self.id, curDate)

        with open(filename, "w+", encoding='utf-8') as f:
            f.write(todayData)

    def TodayDataColl(self):
        '''
        采集数据并转换成[{'itemid':'aaaa','obsvalue':'bbbb'},{'itemid':'aaaa','obsvalue':'bbbb'}]
        如果是秒采样，转成分采样
        '''
        order = "get /23+{0}+dat+1+0 /http/1.1".format(self.id)
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        todayData = self.tuoke(2048)
        data = todayData.replace("  ", " ").split(" ")
        samplerate = data[3]
        num = int(data[4])
        data = data[5:]
        tem = []  # 最终放置[ [],[],[] ]
        resultReturn = []  # 放置最终结果[ {},{},{} ]
        db = Database()
        db.ConnectDatabase()
        itemunit = []
        for i in range(num):
            resultReturn.append({'itemid': data[i]})
            tem.append([])
            rs = db.SelectData("select unit from qz_dict_items where itemid='{0}'".format(data[i]))
            rs = list(rs)[0][0]
            itemunit.append(rs)
        db.DBclose()
        data = data[num:]

        if samplerate == '01':
            '分采样'
            for i, j in enumerate(data):
                tem[i % num].append(j)
        if samplerate == '02':
            '把秒采样数据转成分采样'
            try:
                value = 0
                while True:
                    for i in range(num):
                        tem[(value % num) + i].append(data[value + i])
                    value += 60 * num
            except IndexError:
                pass

        for i in range(num):
            "转换成[['2221','abcd'],['2222','abcd']]"
            resultReturn[i]['obsvalue'] = ' '.join(tem[i])
            resultReturn[i]['dataimg'] = ToImageBin(tem[i], itemunit[i])
        return resultReturn

    def MeasurementParameter(self):
        order = "get /21+{0}+pmr+m /http/1.1".format(self.id)
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        todayData = self.tuoke(2048)
        print(todayData)

    def __call__(self):
        self.Connect()
        self.Status()


def ToFloat(x):
    if x != 'NULL':
        return float(x)
    else:
        return np.nan


def ToImageBin(y, unit):
    x = np.arange(0, len(y) / 60, 1 / 60)
    y = np.array(list(map(ToFloat, y)))
    maxy = max(y)
    fig = plt.figure()
    plt.plot(x, y)
    plt.text(0, maxy, unit)
    canvas = fig.canvas

    buffer = io.BytesIO()
    canvas.print_png(buffer)
    data = buffer.getvalue()
    buffer.close()
    plt.close(fig)
    return data

def shiyunxing():
    print('开始')
    import sqlite3
    conn = sqlite3.connect('yq.db')
    c = conn.cursor()
    sql = 'select instrip, instrid, username, password from capacity'
    c.execute(sql)
    while True:
        para = c.fetchone()
        if para is None:
            break
        print("--------------------------------")
        dc = DataCollection(*para)
        dc.Connect()
        if dc.conStatus == 2:
            status = dc.FiveMinuteData()
            dc.out(status)
            dc.Close()

def shiyunxingOne():
    print("--------------------------------")
    dc = DataCollection('10.35.73.164', '431320060939', 'administrator', '01234567')
    dc.Connect()
    if dc.conStatus == 2:
        status = dc.FiveMinuteData()
        dc.out(status)
        dc.Close()

if __name__ == "__main__":
    print("开始")
    shiyunxingOne()
    #dc = DataCollection("10.35.185.100", "9100DQYL0308", "administrator", "01234567")
    #dc = DataCollection("10.35.180.253", "X212MGPH0144", "administrator", "01234567")
    #dc = DataCollection("10.35.179.130", "X212MGPH0147", "administrator", "01234567")
    #dc = DataCollection("10.35.185.111", "J222DQYQ9548", "administrator", "01234567")
    #dc = DataCollection("10.35.185.112", "X212MGPH0143", "administrator", "01234567")
    #dc = DataCollection("10.35.177.125", "X222WHYQ3056", "admin", "xm361003")
    #dc = DataCollection("10.35.186.103", "X312JSEA1803", "administrator", "01234567")
    #dc = DataCollection("10.35.184.101", "X312JSEA1005", "administrator", "01234567")
    #dc = DataCollection("10.35.178.20", "X212MGPH0092", "administrator", "01234567")
    #dc.Connect()
    #if dc.conStatus == 2:
        #todaydata = dc.TodayDataColl()
        #status = dc.Status()
        #curData = dc.FiveMinuteData()
        #print(status,curData)
        #data1 = dc.MeasurementParameter()
        #print(data1)
        #p = Process(target=dc.RealTimeData, args=())
        #p.start()
        #dc.TodayData()
        #p.join()
        #dc.FiveMinuteData()
        #print(status, curData, data1)
    #print(dc.conStatus, dc.sendStatus, dc.recvStatus, dc.dataFormatStatus)
    print("结束")