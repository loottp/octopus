# -*- coding:utf-8 -*-
'''
select  instrid,instrip ,stationid, pointid from qz_dict_stationinstruments
入口主函数
'''
import cx_Oracle


class Database():
    "数据库类"
    def __init__(self):
        self.db = None

    def ConnectDatabase(self):
        self.db = cx_Oracle.connect('qzdata', '19820604', '10.35.179.95/PDBQZ')

    def SelectData(self, sql):
        cr = self.db.cursor()
        cr.execute(sql)
        rs = cr.fetchall()
        cr.close()
        return rs

    def InsertOrUpdateData(self, sql, param = None):
        cr = self.db.cursor()
        if param == None:
            cr.execute(sql)
        else:
            cr.execute(sql,param)
        self.db.commit()
        cr.close()

    def InsertOrUpdateManyData(self, sql, param = None):
        cr = self.db.cursor()
        cr.executemany(sql, param)
        self.db.commit()
        #cr.execute('commit')
        cr.close()


    def DBclose(self):
        self.db.close()