# 测试函数

from collectData import *
import random

conn = sqlite3.connect('../web_oct/yq.db')
c = conn.cursor()
yqinfo = list(c.execute("SELECT * FROM CAPACITY WHERE network = 0  "))
si = list(c.execute("SELECT * FROM SI"))
def initialise():
    for i in si:
        abnormalTab['networks'][i[0]] = {'stationname':i[1], 'instrnums':i[2], 'instrgateway':i[3], 'abn_instr_nums':0, 'abngateway':0, 'stationnetwork':0}

    for i in yqinfo:
        attributeTab[i[1]] = {'instrip': i[0].strip(), 'stationid':i[2], 'pointid':i[3], 'username':i[4], \
                              'password':i[5], 'instrproject':i[6], 'stationname':i[7],'instrname':i[8],\
                              'instrtype':i[9], 'samplerate': i[10], 'itemnum':i[11], 'network':i[12],\
                              'connection':i[13], 'login': i[14], 'yqstatus': i[15], 'realdata':i[16],\
                              'todaydata':i[17], 'fivedata':i[18], 'working_parameterNum':i[20], \
                              'working_parameter':i[21], 'items':i[22], 'working_parameter_kg':i[23], 'mainitems':i[24], \
                              'threshold':i[25]}
        statusTab[i[1]] = {'network': 0, 'status': None, 'connection':None, 'login':None, 'importData':None}
        realdataTab[i[1]] = {'lastTime': None, 'length':0, 'data': []}
        abnormalTab['yiqi'][i[1]] = {'overrange':[]}
def import_data_to_realdataTab():
    while True:
        starttime = time.time()
        for i in yqinfo:
            try:
                num = attributeTab[i[1]]['itemnum']
                if num == None:
                    continue
                t = int(time.time())
                tem = []
                for j in range(num):
                    data = str(random.randint(1000, 2000))
                    tem.append(data)
                realdataImport(i[1], [t, tem])

            except:
                print("测试实时数据时出现异常")
        time.sleep(1)

def test_realdata(yqinfo):
    while True:
        try:
            for i in yqinfo:
                pass
        except:
            print("测试实时数据时出现异常")

if __name__ == "__main__":
    print('开始1')
    initialise()
    t1 = threading.Thread(target=import_data_to_realdataTab, args=())
    threads.append(t1)

    t4 = threading.Thread(target=Watchdog, args=(yqinfo, si))
    threads.append((t4))
    for i in threads:
        i.start()
    print("结束")
    pass