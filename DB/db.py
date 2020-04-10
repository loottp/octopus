import collectFromYQ.operaDB

from DB.database import *
import sqlite3

dbOracle = Database()
dbOracle.ConnectDatabase()
sqlSelectYQ = '''select  a.instrip, a.instrid ,a.stationid, a.pointid ,a.username, a.password ,a.instrProject,
        b.stationname,  c.instrname, c.instrtype , a.instrGateway from qz_dict_stationinstruments a, qz_dict_stations b, 
        qz_dict_instruments c where a.stationid=b.stationid and a.instrcode=c.instrcode  
        and enddate is null and a.instrip !='127.0.0.1' '''

# yqInfos的顺序：ip|id|stationid|pointid|username|password|instrProject|stationname|instrname|instrtype|instrgateway
yqInfos = list(dbOracle.SelectData(sqlSelectYQ))
conn = sqlite3.connect('../web_oct/yq.db')
c = conn.cursor()

"""
for i in yqInfos:
    c.execute("UPDATE CAPACITY SET INSTRGATEWAY ='%s' WHERE INSTRID='%s'" % (i[10], i[1]))
    conn.commit()
"""
s = {}
for i in yqInfos:
    if s.get(i[2]) is None:
        s[i[2]] = [i[2], i[7], 1, i[10]]
    else:
        s[i[2]][2] += 1
for i in s.values():
    c.execute('Insert Into SI values (?,?,?,?)',i)
conn.commit()

conn.close()
dbOracle.DBclose()
