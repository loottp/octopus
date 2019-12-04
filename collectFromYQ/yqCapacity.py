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
检测仪器能力的类，比如网络、数采状态、实时数据、当天数据、时钟、当前值从网页、时钟从网页
"""
class capacity:
    def __init__(self, yqInfos):
        self.conn = sqlite3.connect('yq.db')
        self.c = self.conn.cursor()
        self.yqInfos = yqInfos      #仪器信息列表


    def dbclose(self):
        self.conn.close()

    def insertToSql3(self):
        self.c.executemany('insert into capacity values (?,?,?,?,?,?,?,?,?,?,null,null,null,null,null,null,null) ', self.yqInfos)
        self.conn.commit()


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
        s = socket.socket()
        s.settimeout(30)

        for i in self.yqInfos:
            try:
                s.connect((i[0],81))
            except TimeoutError:


            DC = DataCollection(i[0], i[1], i[2], i[3])
            DC.Connect()
            t = DC.Status()
            if t == None:

    def updateRealData(self):
        pass

    def updateTodayData(self):
        pass





