# -*- coding:utf-8 -*-
'''
select  instrid,instrip ,stationid, pointid from qz_dict_stationinstruments
入口主函数
'''
from datacollection import *
import database
import time
import icmp
import databaseTables

def isShiWu(args):
    if args[6]==1 and args[0]!='127.0.0.1':
        return True
    else:
        return False

def MainYqStatus():
    "主函数"
    print("开始")
    try:
        DB = database.Database()
        DB.ConnectDatabase()
        sqlSelectYQ = '''select  a.instrip, a.instrid ,a.stationid, a.pointid ,a.username, a.password ,a.instrProject, 
        b.stationname,  c.instrname, c.instrtype from qz_dict_stationinstruments a, qz_dict_stations b, 
        qz_dict_instruments c where a.stationid=b.stationid and a.instrcode=c.instrcode'''
        rs = DB.SelectData(sqlSelectYQ)
        rs = list(filter(isShiWu, rs))
        print(rs)
        '''rs = [('10.35.180.103  ', 'X341IOES3072', '35003', '3', 'administrator', '01234567', 1),
              ('10.35.180.104  ', '9100DQYL0285', '35003', '4', 'administrator', '01234567', 1),
              ('10.35.180.102  ', '3120IGEA4040', '35003', '2', 'administrator', '01234567', 1),
              ('10.35.180.101  ', '3120IGEA1040', '35003', '1', 'administrator', '01234567', 1)]'''
        for i in rs:
            yqIP = i[0].strip()  # ip地址
            yqID = i[1]  # 仪器id
            yqUsername = i[4]  # 仪器用户名
            yqPassword = i[5]  # 仪器密码
            stationid = i[2]  # 台站代码
            pointid = i[3]  # 测点编码
            tablestatus = databaseTables.TableStatus(DB)
            print("IP地址：", yqIP)
            tablestatus.setYQ(stationid, pointid)
            netStatus = icmp.ping(yqIP)
            print(netStatus)
            tablestatus.setNetStatus(netStatus)
            if netStatus == 1:
                tablestatus.InsertOrUpdate()
                continue
            else:
                dc = DataCollection(yqIP, yqID, yqUsername, yqPassword)
                try:
                    linkStatus = dc.Connect()
                    if linkStatus == b'$nak\n' or linkStatus == b'$err\n' or linkStatus == b'$err\r\n':
                        tablestatus.setYqConn(1)
                        tablestatus.InsertOrUpdate()
                        continue
                except TimeoutError:
                    tablestatus.setYqConn(1)
                    tablestatus.InsertOrUpdate()
                    continue
                tablestatus.setYqConn(0)
                statusColl = dc.Status()  # 通过仪器采集到的状态信息
                curData = dc.FiveMinuteData()  # 通过仪器采集到的当前数据
                if statusColl != None:
                    tablestatus.setYqStatus(statusColl)
                '''if curData != None:
                    tablestatus.setCurData(curData)'''
                tablestatus.InsertOrUpdate()
            print("插入成功")
    finally:
        DB.DBclose()
    print("结束")

def MainYqData():
    print("开始")
    try:
        DB = database.Database()
        DB.ConnectDatabase()
        sqlSelectYQ = '''select  a.instrip, a.instrid ,a.stationid, a.pointid ,a.username, a.password ,a.instrProject, 
        b.stationname,  c.instrname, c.instrtype from qz_dict_stationinstruments a, qz_dict_stations b, 
        qz_dict_instruments c where a.stationid=b.stationid and a.instrcode=c.instrcode'''

        unit = "select unit from qz_dict_items where itemid = itemid"
        rs = DB.SelectData(sqlSelectYQ)
        rs = list(filter(isShiWu, rs))
        print(rs)
        for i in rs:
            yqIP = i[0].strip()  # ip地址
            yqID = i[1]  # 仪器id
            yqUsername = i[4]  # 仪器用户名
            yqPassword = i[5]  # 仪器密码
            stationid = i[2]  # 台站代码
            pointid = i[3]  # 测点编码

            print("IP地址：", yqIP)
            netStatus = icmp.ping(yqIP)
            if netStatus == 1:
                continue
            else:
                dc = DataCollection(yqIP, yqID, yqUsername, yqPassword)
                try:
                    linkStatus = dc.Connect()
                    if linkStatus == b'$nak\n' or linkStatus == b'$err\n' or linkStatus == b'$err\r\n':
                        continue
                except TimeoutError:
                    continue
            try:
                todayData = dc.TodayDataColl()
            except Exception as e:
                print(e.args[0])
                continue

            tableYqData = databaseTables.TableYqData(DB)
            tableYqData.setYQ(stationid, pointid)
            tableYqData.InsertOrUpdate(todayData)


    except Exception as e:
        print(e.args[0])
    finally:
        DB.DBclose()
    print('结束')

def ceshi():

    DB = database.Database()
    DB.ConnectDatabase()
    file = open("ceshitupian.jpg",'rb')
    param = [file.read()]
    sqlin = '''insert into qz_cxtd_yqdata
    (startdate,
     stationid,
     pointid,
     itemid,
     SAMPLERATE,
     dataimg    )
  values
    (to_date(20190419103920,'yyyymmddHH24miss'), '35011', '1', '2210', '01', (:1)) '''

    DB.InsertOrUpdateData(sqlin,param)
    DB.DBclose()


if __name__=='__main__':
    #运行主函数

    while(1):
        #MainYqStatus()
        #ceshi()
        MainYqData()
        time.sleep(300)
    

