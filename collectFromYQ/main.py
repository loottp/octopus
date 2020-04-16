# 主函数

from collectData1 import *

def Main():
    conn = sqlite3.connect('../web_oct/yq.db')
    c = conn.cursor()
    yqinfo = list(c.execute("SELECT * FROM CAPACITY WHERE network = 0  "))
    si = list(c.execute("SELECT * FROM SI"))
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


    # PING网络
    t1 = threading.Thread(target=Network, args=(yqinfo, ))
    threads.append(t1)

    # 采集状态、部分五分钟数据
    t2 = threading.Thread(target=Status_dict, args=(yqinfo,))
    threads.append(t2)

    # 采集实时数据
    for i in yqinfo:
        if i[16] == 0:
            t = threading.Thread(target=Realdata_dict, args=(i,))
            threads.append(t)


    #采集南平九五数据
    t3 = threading.Thread(target=JW_np)
    threads.append(t3)

    #看门狗
    t4 = threading.Thread(target=Watchdog, args=(yqinfo, si))
    threads.append((t4))

    for i in threads:
        i.start()

if __name__ == "__main__":
    print('开始1')
    Main()
    print("结束")