#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
'''========================================================================================================================================
 1 数据库的连接信息，如果有变动，在此改动 , 格式固定通用。后续写成包时，可以使用其进行调用,确定环境后进行替换
 我这里是对同一个库，同一个表里数据的操作和存储。
 '''

host = "ip地址"
user = "用户名"
port = 3306
passwd = '密码'
database = '数据库'
charset = 'utf8'

'''1 数据库增删改查'''
def del_sql(sql):
    db = pymysql.connect(host=host,
                         user=user,
                         port=port,
                         passwd=passwd,
                         database=database,
                         charset=charset)
    cursor = db.cursor()
    cursor.execute(sql)
    cursor.close()
    db.commit()
    db.close()

'''2 获得数据库数据'''
def get_sql_data(sql):
    db = pymysql.connect(host=host,
                         user=user,
                         port=port,
                         passwd=passwd,
                         database=database,
                         charset=charset)
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    db.commit()
    db.close()
    col_name_list = [tuple[0] for tuple in cursor.description]
    data = list(data)
    data = [list(i) for i in data]
    datas = pd.DataFrame(data, columns=col_name_list)
    return datas

'''    3 数据直接存入数据库'''
def to_sql(df, to_filename):
    engine = create_engine(str(r'mysql+pymysql://%s:%s@%s:%s/%s?charset=%s' % (database, passwd, host, port, user, charset)))
    df.to_sql(to_filename, con=engine, if_exists='append', index=False)  # 增量入库，不会覆盖之前的数据
