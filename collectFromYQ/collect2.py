from collectData import *

class DataCollect2(DataCollection):
    def WorkParameters(self):
        '采集状态仪器状态信息，通过get /19+id+ste /http/1.1'
        print("开始采集仪器工作参数", self.info[0:4],self.info[7:10])
        order = "get /21+{0}+pmr+m /http/1.1".format(self.info[1])
        # 测试仪器状态
        self.conInstrument.sendall(bytes(order, encoding="utf-8"))
        try:
            tem_parameters = self.conInstrument.recv(200)
            print(tem_parameters)
            if b'ack\n' in tem_parameters:
                self.o_yqstatus = 0

                return tem_parameters.decode()
            elif tem_parameters == b'$nak\n':
                self.o_yqstatus = 1
                return None
            elif tem_parameters == b'$err\n':
                self.o_yqstatus = 2
                return None
            else:
                self.o_yqstatus = 4
                return None
        except socket.timeout:
            self.o_yqstatus = 3
            return None
        except:
            self.o_yqstatus = 3
            return None

if __name__ == "__main__":
    try:
        conn = sqlite3.connect('../web_oct/yq.db')
        c = conn.cursor()
        yqinfo = list(c.execute("SELECT * FROM CAPACITY  where instrtype = 'VP' "))
        for i in yqinfo:
            dc = DataCollect2(i)
            dc.Connect()
            if dc.o_connect == 0:
                dc.Login()
                if dc.o_login == 0:
                    try:
                        print('开始采集五分钟数据')
                        #data = dc.FiveData_1()
                        data = dc.RealTimeOneData()
                        if data is not None:
                            data = data.split('\n')[1].split(' ')
                            num = int(data[5])
                            print('五分钟数据', data, int(data[5]), ' '.join(data[6:6+num]))
                            print(1)
                            try:
                                c.execute("UPDATE CAPACITY SET items='%s' WHERE INSTRID='%s'" % (' '.join(data[6:6+num]), i[1]))
                            except Exception as e:
                                print(e.args)
                        print('开始参数数据')
                        data = dc.WorkParameters()

                        if data is not None:
                            data = data.split('\n')[1].split(' ')
                            print('参数数据', data)
                            num = int(data[3])
                            print(num,' '.join(data[4:]))
                            c.execute("UPDATE CAPACITY SET workingparameternum =%d, workingparameter = '%s' WHERE INSTRID='%s'" % (num, ' '.join(data[4:4+num]), i[1]))
                        conn.commit()
                    except:
                        continue
        conn.commit()
        conn.close()
    except:
        pass