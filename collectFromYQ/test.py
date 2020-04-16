# 测试函数

from collectData import *
import random

conn = sqlite3.connect('../web_oct/yq.db')
c = conn.cursor()
yqinfo = list(c.execute("SELECT * FROM CAPACITY WHERE network=0 and realdata=0  "))
si = list(c.execute("SELECT * FROM SI"))

class TestReal(DataCollection):
    # 实时采集数据
    def RealTimeData1(self, bq):
        self.conInstrument.settimeout(100)
        print(threading.currentThread().ident)
        print("开始测试仪器实时数据")
        order = "get /21+{0}+dat+0 /http/1.1".format(self.info[1])
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        tem_real = b''
        noneLength = 0
        while True:
            try:
                tem_real += self.conInstrument.recv(300)
                content_ack = re.match(b'\$(.*?)ack\n', tem_real, re.DOTALL)            #加re.DOTALL代表匹配所有字符包括\n

                if content_ack is not None:
                    tem_real = tem_real[content_ack.span()[1]:]         #把剩下的还给tem_real

                    bq.put([int(time.time()), content_ack.group().decode()])
                    self.o_realdata = 0
                    noneLength = 0
                else:
                    content_nak = re.match(b'\$nak\n', tem_real, re.DOTALL)
                    if content_nak is not None:
                        self.o_realdata = 1
                        msg = '实时数据收到nak'
                        self.Warning(msg)
                        self.Close()
                        break
                    else:
                        content_err = re.match(b'\$err\n', tem_real, re.DOTALL)
                        if content_err is not None:
                            self.o_realdata = 2
                            msg = '实时数据收到err'
                            self.Warning(msg)
                            self.Close()

                        elif tem_real == b'':
                            noneLength += 1
                            if noneLength > 100:
                                # 报警
                                msg = '采集实时数据时变量tem_reals收到空数据大于300，仪器端已经断开'
                                self.Warning(msg)
                                self.Close()
                                break

                        elif len(tem_real) > 600:
                            # 报警
                            msg = '采集实时数据出现异常,变量tem_reals数据大于600' + tem_real.decode()
                            self.Warning(msg)
                            self.Close()

                            break

            except socket.timeout:
                # 报警
                msg = '采集实时数据超时，发生了阻塞'
                self.Warning(msg)
                self.Close()
                break
            except:
                s = traceback.format_exc()
                logging.error(s)
                # 报警
                msg = '出现异常,变量tem_real:' + tem_real.decode()
                self.Warning(msg)
                self.Close()
                break

def real_consumer(bq):
    #time.sleep(5)
    while True:
        tem = bq.get()
        print(tem)
        datatem = tem[1].split('\n')[1].split(' ')
        itemnum = int(datatem[5])
        data = [tem[0], datatem[-itemnum:]]
        id = datatem[3]
        realdataImport(id, data)

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
                    data = str(random.randint(-500, 500))
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

def Realdata_dict(yqOne, bq):
    while True:
        print('开始实时数据采集')
        dc = TestReal(yqOne)
        if statusTab[yqOne[1]]['network'] == 0:
            dc.Connect()
            if dc.o_connect == 0:
                statusTab[yqOne[1]]['connection'] = 0
                dc.Login()
                if dc.o_login == 0:
                    statusTab[yqOne[1]]['login'] = 0
                    dc.RealTimeData1(bq)
        print('实时数据采集无网络或，意外停止，暂停5分钟')
        time.sleep(300)

if __name__ == "__main__":
    print('开始1')
    initialise()
    bq = queue.Queue(maxsize=0)
    '''
    t1 = threading.Thread(target=import_data_to_realdataTab, args=())
    threads.append(t1)

    t4 = threading.Thread(target=Watchdog, args=(yqinfo, si))
    threads.append((t4))
    '''
    for i in yqinfo:
        if i[16] == 0:
            t = threading.Thread(target=Realdata_dict, args=(i, bq))
            threads.append(t)
    t3 = threading.Thread(target=real_consumer, args=(bq, ))
    for i in threads:
        i.start()
    t3.start()
    print("结束")
    pass