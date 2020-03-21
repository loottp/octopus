import socket
s = socket.socket()
ip = '10.35.180.230'
id = '222XWHYQ0535'
user = 'administrator'
password = '01234567'
s.connect((ip, 81))
order = "get /42+{0}+lin+{1}+{2} /http/1.1".format(id, user, password)
s.sendall(bytes(order, encoding="utf-8"))
temp = s.recv(10)
print(temp)

order = "get /21+{0}+dat+5 /http/1.1".format(id)
s.sendall(bytes(order, encoding="utf-8"))
temp = s.recv(500)
print(temp)
