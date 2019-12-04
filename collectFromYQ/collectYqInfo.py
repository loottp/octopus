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
        self.conInstrument = socket.socket()  # 套接字对象，通过socket
        self.conInstrument.settimeout(30)

    def recv_end(self, length):
        '尾标识方法，备用'
        End = b"\nack\n"
        total_data = []
        data = b''
        while True:
            data = self.conInstrument.recv(length)
            if End in data:
                total_data.append(data)
                break
            total_data.append(data)
            if len(total_data) > 1:
                if End in total_data:
                    break
        return b''.join(total_data).decode()

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
        except TimeoutError:
            print(self.ip, " ", self.id, '无法连接ping不通')
        else:
            order = "get /42+{0}+lin+{1}+{2} /http/1.1".format(self.id, self.user, self.password)
            self.conInstrument.sendall(bytes(order, encoding="utf-8"))
            tem = self.conInstrument.recv(100)
            if tem == b'$err\n':
                print("仪器无法连上", self.ip, self.id)

            return tem

    # 采集状态信息
    def Status(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        order = "get /19+{0}+ste /http/1.1".format(self.id)
        try:
            self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        except socket.timeout:
            print(self.ip, " ", self.id, "发送数据超时")
        else:
            tem = self.tuoke(100)
            if tem == None:
                return None
            else:

                tem = tem.replace('  ', ' ').split(" ")
                # print(tem)
                # 删除列表中序号为2,5,6,9以及10（如果有），从尾巴开始删除元素
                try:
                    del tem[10]
                except:
                    pass
                del tem[9]
                del tem[5:7]  # 5,6但不包括7
                del tem[2]
                print(tem)
                return tem

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
        order = "get /21+{0}+dat+5 /http/1.1".format(self.id)
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        # tem = self.conInstrument.recv(200)
        tem = self.tuoke(200)
        if tem == None:
            return None
        else:
            tem = tem.replace('  ', ' ').split(" ")  # 有的字符串中有两个空格，replace去除两个空格
            itemLength = int(tem[4])
            result = tem[-itemLength:]
            print(result)
            if itemLength == 1:
                return result[0]
            else:
                result = ' '.join(result)
                return result

    # 终止实时数据
    def StopData(self):
        order = "get /19+{0}+stp /http/1.1".format(self.id)
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        tem = self.conInstrument.recv(200)
        print(tem)

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