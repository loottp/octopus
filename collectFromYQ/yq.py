import socket
s = socket.socket()
ip = '10.35.180.101'
id = '3120IGEA1040'
user = 'administrator'
password = '01234567'
s.connect((ip, 81))
order = "get /42+{0}+lin+{1}+{2} /http/1.1".format(id, user, password)
s.sendall(bytes(order, encoding="utf-8"))
temp = s.recv(10)
print(temp)

order = "get /19+{0}+ste /http/1.1".format(id)
s.sendall(bytes(order, encoding="utf-8"))
temp = s.recv(100)
print(temp)
