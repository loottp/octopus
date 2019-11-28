# 操作sqlite3数据，用于存储临时数据
'''
select  instrid,instrip ,stationid, pointid from qz_dict_stationinstruments
入口主函数
'''
import cx_Oracle
import sqlite3


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


# 创建能力表
createCapacityTable = '''CREATE TABLE CAPACITY 
        (
        INSTRIP CHAR(15) ,
        INSTRID CHAR(12) PRIMARY KEY NOT NULL,
        STATIONID CHAR(5),
        POINTID CHAR(1),
        USERNAME VARCHAR(20),
        PASSWORD VARCHAR(10),
        INSTRPROJECT INT,
        STATIONNAME TEXT,
        INSTRNAME TEXT,
        INSTRTYPE VARCHAR(20),
        NETWORK CHAR(1) ,
        COLLECTION CHAR(1),
        CURRENTDATA CHAR(1),
        TODAYDATA CHAR(1),
        CLOCK CHAR(1),
        CDFROMWEB CHAR(1),
        CLOCKFROMWEB CHAR(1));
        '''
# 能力表创建记录
insertCapacityTable = "INSERT INTO CAPACITY ({0}) VALUES ({1})"

class sql3:
    def __init__(self, db):
        self.db = db
        self.conn = None
        self.c = None

    def connDB(self):
        self.conn = sqlite3.connect('yq.db')
        print("打开数据库yq.db成功")
        self.c = self.conn.cursor()

    def operaTableOne(self, sql):
        self.c.execute(sql)
        self.conn.commit()
        print("创建数据库成功")

    def operaTableMany(self, sql, value):
        self.c.executemany(sql, value)
        self.conn.commit()
        print("插入数据表成功")

    def closeDB(self):
        self.conn.close()

# 创建表
def createTableYQ(db):
    db = sql3(db)
    db.connDB()
    db.operaTableOne(createCapacityTable)
    db.closeDB()

# 删除数据表
dropTable = 'DROP TABLE CAPACITY'
def delTable(db):
    db = sql3(db)
    db.connDB()
    db.operaTableOne(dropTable)
    db.closeDB()

if __name__ == '__main__':
    createTableYQ('yq.db')
    #delTable('yq.db')
    print('开始数据库')

