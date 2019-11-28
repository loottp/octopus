#
# 测试仪器是否有能力收集到信息
# 包括：网络是否通，数采信息，当天数据，实时数据
# 保存在capacity表中
#

from operaDB import *
import collectNetInfo
import collectYqInfo
import icmp

dbOracle = Database()
dbOracle.ConnectDatabase()
sqlSelectYQ = '''select  a.instrip, a.instrid ,a.stationid, a.pointid ,a.username, a.password ,a.instrProject,
        b.stationname,  c.instrname, c.instrtype from qz_dict_stationinstruments a, qz_dict_stations b, 
        qz_dict_instruments c where a.stationid=b.stationid and a.instrcode=c.instrcode  
        and enddate is null and a.instrip !='127.0.0.1' '''
yqInfos = list(dbOracle.SelectData(sqlSelectYQ))
dbOracle.DBclose()

dbSql3 = sql3('yq.db')
dbSql3.connDB()
dbSql3.operaTableMany('insert into capacity values (?,?,?,?,?,?,?,?,?,?,null,null,null,null,null,null,null) ', yqInfos)




dbSql3.closeDB()


