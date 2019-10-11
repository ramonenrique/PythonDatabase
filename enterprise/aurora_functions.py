from credentials import *
import pymysql

def connect_me_to_aurora():
    conn_aurora = pymysql.connect(host=aurora_host, port=aurora_port, user=aurora_user, passwd=aurora_passwd, db=aurora_db)
    return conn_aurora