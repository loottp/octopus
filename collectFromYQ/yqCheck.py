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
        self.conn = sqlite3.connect('yq.db')
        self.c = self.conn.cursor()
        self.network = None
        self.collection = None



    def dbclose(self):
        self.conn.close()


    def checkYQ(self):
        # 检查网络
        yqinfo = list(self.c.execute("SELECT INSTRIP,INSTRID,USERNAME,PASSWORD,INSTRPROJECT FROM CAPACITY WHERE INSTRPROJECT=0"))
        for i in yqinfo:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            d = ping(i[0].strip(), timeout=2, count=2)
            if d == 0:
                print(i[0]+'ip', "ok")
                o_network = 0
                try:
                    # 测试连接
                    s.connect((i[0].strip(), 81))
                    o_connect = 0
                except:
                    print(i[0], '无法连接')
                    self.c.execute("UPDATE CAPACITY SET CONNECTION=1 WHERE INSTRID='%s'" % i[1])
                    self.conn.commit()
                else:

                    # 测试登录
                    order = "get /42+{0}+lin+{1}+{2} /http/1.1".format(i[1], i[2], i[3])
                    s.sendall(bytes(order, encoding="utf-8"))
                    tem = s.recv(10)
                    if tem == b'$ack\n':
                        o_login = 0
                        """
                        # 测试仪器状态
                        order = "get /19+{0}+ste /http/1.1".format(i[1])
                        try:
                            s.sendall(bytes(order, encoding="utf-8"))
                        except:
                            print(i[0], '收集仪器状态出现异常')
                        else:
                            tem_status = s.recv(100)
                            print(tem_status)
                            if b'ack\n' in tem_status:
                                o_yqstatus = 0
                            else:
                                o_yqstatus = 1
                        

                        # 测试实时数据
                        order = "get /21+{0}+dat+0 /http/1.1".format(i[1])
                        try:
                            s.sendall(bytes(order, encoding="utf-8"))
                        except:
                            print(i[0], '实时数据出现异常')
                        else:
                            tem_real = s.recv(300)
                            print(tem_real)
                            if b'ack\n' in tem_real:
                                o_realdata = 0
                                self.c.execute("UPDATE CAPACITY SET REALDATA=0 WHERE INSTRID='%s'" % i[1])
                                self.conn.commit()
                            else:
                                o_realdata = 1
                                self.c.execute("UPDATE CAPACITY SET REALDATA=1 WHERE INSTRID='%s'" % i[1])
                                self.conn.commit()

                            # 停止实时数据
                            order = "get /19+{0}+stp /http/1.1".format(i[1])
                            s.sendall(bytes(order, encoding="utf-8"))
                            tem_stop = s.recv(100)
                        


                        #不包括九五仪器
                        if i[4]==1:
                            #测试当天数据
                            order = "get /23+{0}+dat+1+0 /http/1.1".format(i[1])
                            try:
                                s.sendall(bytes(order, encoding="utf-8"))
                            except:
                                print(i[0], '当天数据出现异常')
                            else:
                                tem_todaydata = []
                                while True:
                                    data = s.recv(50000)

                                    if not data: break
                                    tem_todaydata.append(data)

                                    tem1 = b''.join(tem_todaydata)
                                    print(tem1)
                                    if b'ack\n' in tem1:
                                        self.c.execute("UPDATE CAPACITY SET TODAYDATA=0 WHERE INSTRID='%s'" % i[1])
                                        self.conn.commit()
                                        break
                                    if b'$err' in tem1:
                                        self.c.execute("UPDATE CAPACITY SET TODAYDATA=1 WHERE INSTRID='%s'" % i[1])
                                        self.conn.commit()
                                        break
                                        
                            #测试五分钟数据
                            order = "get /21+{0}+dat+5 /http/1.1".format(i[1])
                            try:
                                s.sendall(bytes(order, encoding="utf-8"))
                            except:
                                print(i[0], '五分钟数据出现异常')
                            else:
                                tem_fivedata = s.recv(500)
                                print(tem_fivedata)
                                if b'ack\n' in tem_fivedata:
                                    self.c.execute("UPDATE CAPACITY SET FIVEDATA=0 WHERE INSTRID='%s'" % i[1])
                                    self.conn.commit()
                                else:
                                    self.c.execute("UPDATE CAPACITY SET FIVEDATA=1 WHERE INSTRID='%s'" % i[1])
                                    self.conn.commit()
                        else:
                            continue
                        """





                    else:
                        print('无法登录', tem)
                        self.c.execute("UPDATE CAPACITY SET LOGIN=1 WHERE INSTRID='%s'" % i[1])
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






