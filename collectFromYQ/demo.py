import socket
from icmp_ping import *
import sqlite3
import time
import matplotlib.pyplot as plt
import numpy as np
import io
from DB.database import Database


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(10)
s.connect(('10.35.186.102', 81))
order = "get /42+X341IOES3334+lin+administrator+01234567 /http/1.1"
s.sendall(bytes(order, encoding="utf-8"))
a = s.recv(100)
print(a)
# 重启仪器
order = "get /19+X341IOES3334+rbt11 /http/1.1"
s.sendall(bytes(order, encoding="utf-8"))
a = s.recv(100)
print(a)
