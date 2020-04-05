#
# 测试仪器是否有能力收集到信息
# 包括：网络是否通，数采信息，当天数据，实时数据
# 保存在capacity表中
#

from operaDB import *
import collectNetInfo
import collectYqInfo
from icmp_ping import *
import sqlite3
from collectYqInfo import DataCollection
import time
"""
dbOracle = Database()
dbOracle.ConnectDatabase()
sqlSelectYQ = '''select  a.instrip, a.instrid ,a.stationid, a.pointid ,a.username, a.password ,a.instrProject,
        b.stationname,  c.instrname, c.instrtype from qz_dict_stationinstruments a, qz_dict_stations b, 
        qz_dict_instruments c where a.stationid=b.stationid and a.instrcode=c.instrcode  
        and enddate is null and a.instrip !='127.0.0.1' '''

# yqInfos的顺序：ip|id|username|password|instrProject|stationname|instrname|instrtype
yqInfos = list(dbOracle.SelectData(sqlSelectYQ))
results = {}                #为最终结果
for i in yqInfos:
    i = list(i)
    i[0] = i[0].strip()
    results[i[1]] = i
dbOracle.DBclose()
print(results)
"""

"""
检测仪器能力的类，比如网络、登陆、数采状态、实时数据、当天数据、时钟、当前值从网页、时钟从网页
数据库capacity：0代表正常，1代表异常， 2代表状态未定
"""
class check:
    def __init__(self):
        self.conn = sqlite3.connect('../web_oct/yq.db')
        self.c = self.conn.cursor()
        self.network = None
        self.collection = None



    def dbclose(self):
        self.conn.close()


    def checkYQ(self):
        # 检查网络
        file = open('d:/1.txt', 'w+')
        yqinfo = list(self.c.execute("SELECT INSTRIP,INSTRID,USERNAME,PASSWORD,INSTRPROJECT,INSTRTYPE FROM CAPACITY where instrid='3120SNMY1122'"))
        print(yqinfo, len(yqinfo))
        o_network = 0
        o_connect = 0
        o_login = 0
        o_yqstatus = 0
        o_realdata = 0
        o_todaydata = 0
        o_fivedata = 0
        for i in yqinfo:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            d = ping(i[0].strip(), timeout=2, count=2)
            if d == 0:
                print(i[0]+'ping', "ok")
                o_network = 0

                # 测试连接
                try:
                    print("开始测试仪器连接")
                    s.connect((i[0].strip(), 81))
                    o_connect = 0
                except:
                    print(i[0], '无法连接')
                    self.c.execute("UPDATE CAPACITY SET CONNECTION=1 WHERE INSTRID='%s'" % i[1])
                    self.conn.commit()
                else:
                    # 测试登录
                    print("开始测试仪器登录")
                    order = "get /42+{0}+lin+{1}+{2} /http/1.1".format(i[1], i[2], i[3])
                    s.sendall(bytes(order, encoding="utf-8"))

                    # 测试仪器状态、实时数据、当天数据、五分钟数据
                    try:
                        tem = s.recv(10)
                        if tem == b'$ack\n':
                            o_login = 0

                            # 测试仪器状态
                            print("开始测试仪器状态", i[5])
                            order = "get /19+{0}+ste /http/1.1".format(i[1])
                            s.sendall(bytes(order, encoding="utf-8"))
                            try:
                                tem_status = s.recv(100)
                                print(tem_status)
                                if b'ack\n' in tem_status:
                                    o_yqstatus = 0
                                elif tem_status == b'$nak\n':
                                    o_yqstatus = 1
                                elif tem_status == b'$err\n':
                                    o_yqstatus = 2
                                else:
                                    o_yqstatus = 4
                            except socket.timeout:
                                o_yqstatus = 3
    
                            
                            # 测试实时数据
                            print("开始测试仪器实时数据")
                            order = "get /21+{0}+dat+0 /http/1.1".format(i[1])
                            s.sendall(bytes(order, encoding="utf-8"))
                            try:
                                tem_real = s.recv(300)
                                print(tem_real)
                                if b'ack\n' in tem_real:
                                    o_realdata = 0
                                    # 停止实时数据
                                    order = "get /19+{0}+stp /http/1.1".format(i[1])
                                    s.sendall(bytes(order, encoding="utf-8"))
                                    tem_stop = s.recv(100)
                                elif tem_real == b'$nak\n':
                                    o_realdata = 1
                                elif tem_real == b'$err\n':
                                    o_realdata = 2
                                else:
                                    o_realdata = 4

                            except socket.timeout:
                                o_realdata = 3
    

                            #不包括九五仪器

                            if i[4] == 1:
                                #测试当天数据
                                print("开始测试仪器当天数据")
                                order = "get /23+{0}+dat+1+0 /http/1.1".format(i[1])
                                s.sendall(bytes(order, encoding="utf-8"))
                                tem_todaydata = []
                                while True:
                                    try:
                                        data = s.recv(50000)
                                        if not data: break
                                        tem_todaydata.append(data)
                                        tem1 = b''.join(tem_todaydata)
                                        if b'ack\n' in tem1:
                                            print(tem1)
                                            o_todaydata = 0
                                            break
                                        elif tem1 == '$nak\n':
                                            o_todaydata = 1
                                        elif tem1 == '$err\n':
                                            o_todaydata = 2
                                        else:
                                            o_todaydata = 4
                                    except socket.timeout:
                                        print("采集当天数据阻塞")
                                        o_todaydata = 3
                                        break

                                #测试五分钟数据
                                print("开始测试仪器五分钟数据")
                                order = "get /21+{0}+dat+5 /http/1.1".format(i[1])
                                s.sendall(bytes(order, encoding="utf-8"))
                                try:
                                    tem_fivedata = s.recv(500)
                                    print(tem_fivedata)
                                    if b'ack\n' in tem_fivedata:
                                        o_fivedata = 0
                                    elif tem_fivedata == '$nak\n':
                                        o_fivedata = 1
                                    elif tem_fivedata == '$err\n':
                                        o_fivedata = 2
                                    else:
                                        o_fivedata = 4
                                except socket.timeout:
                                    o_fivedata = 3

                                # 十五仪器录入数据库
                                self.c.execute(
                                    "UPDATE CAPACITY SET NETWORK=%d,CONNECTION=%d,LOGIN=%d,YQSTATUS=%d,REALDATA=%d,TODAYDATA=%d,FIVEDATA=%d WHERE INSTRID='%s'" % \
                                    (o_network, o_connect, o_login, o_yqstatus, o_realdata, o_todaydata, o_fivedata,i[1]))
                                self.conn.commit()

                            else:
                                # 九五仪器录入数据库
                                self.c.execute(
                                    "UPDATE CAPACITY SET NETWORK=%d,CONNECTION=%d,LOGIN=%d,YQSTATUS=%d,REALDATA=%d WHERE INSTRID='%s'" % \
                                    (o_network, o_connect, o_login, o_yqstatus, o_realdata,i[1]))
                                self.conn.commit()





                        elif tem == b'$nak\n':
                            print('无此命令', tem)
                            self.c.execute("UPDATE CAPACITY SET CONNECTION=0,LOGIN=1 WHERE INSTRID='%s'" % i[1])
                            self.conn.commit()
                        elif tem == b'$err\n':
                            print('命令发送错误',tem)
                            self.c.execute("UPDATE CAPACITY SET CONNECTION=0,LOGIN=2 WHERE INSTRID='%s'" % i[1])
                            self.conn.commit()
                    except socket.timeout:
                        print("登录过程中接收数据发生超时，有阻塞")
                        self.c.execute("UPDATE CAPACITY SET CONNECTION=0,LOGIN=3 WHERE INSTRID='%s'" % i[1])
                        self.conn.commit()
            else:
                print(i[0], "无法ping通")
                self.c.execute("UPDATE CAPACITY SET NETWORK=1 WHERE INSTRID='%s'"%i[1])
                self.conn.commit()
        print(1111)
        self.conn.close()

if __name__ == "__main__":
    c = check()
    c.checkYQ()






