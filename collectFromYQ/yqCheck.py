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

"""
检测仪器能力的类，比如网络、登陆、数采状态、实时数据、当天数据、时钟、当前值从网页、时钟从网页
数据库capacity：0代表正常，1代表异常， 2代表状态未定
"""
class check:
    def __init__(self, ip, id, username, password):
        self.conn = sqlite3.connect('yq.db')
        self.c = self.conn.cursor()
        self.ip = ip      #仪器信息列表
        self.id = id
        self.username = username
        self.password = password
        self.network = None
        self.collection = None
        self.s = socket.socket()


    def dbclose(self):
        self.conn.close()

    def insertToSql3(self):
        self.c.executemany('insert into capacity values (?,?,?,?,?,?,?,?,?,?,null,null,null,null,null,null,null) ', self.yqInfos)
        self.conn.commit()

    def check(self):
        # 检查网络
        d = ping(self.ip, timeout=2, count=1)
        if d == 1:
            d = ping(i[0].strip(), timeout=2, count=4)
            return [1, 2, 2, 2, 2, 2, 2, 2]

        # 检查登陆
        try:
            self.s.connect((self.ip, 81))
        except TimeoutError:
            print(self.ip, " ", self.id, '无法连接ping不通')




    def updateNetwork(self):
        abnList = []            # 网络异常列表
        for i in self.yqInfos:
            d = ping(i[0].strip(), timeout=2, count=1)
            if d == 1:
                abnList.append(i)
                results[i[1]].append(1)
            else:
                results[i[1]].append(0)
        for i in abnList:
            d = ping(i[0].strip(), timeout=2, count=4)
            if d == 0:
                results[i[1]][10] = 0

        v = []
        for i in yqInfos:
            v.append((results[i[1]][10], i[1]))     # results[i][10]为最终的更新network值。
        self.c.executemany('update capacity set network=? where instrid=?', v)

    def updateCollection(self):
        pass

    def updateRealData(self):
        pass

    def updateTodayData(self):
        pass





