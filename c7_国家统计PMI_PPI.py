#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

import pandas as pd
import requests, json
from bs4 import BeautifulSoup
from test_selenium import webdriver
import time
import json
from a0_mysql import del_sql,get_sql_data,to_sql
# from a0_serversql import del_sql,get_sql_data,to_sql  # sql server 数据库
''' ============================================================================================================================='''
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

def gettime():
    return int(round(time.time() * 1000))

def get_df(df_id, dbcode, zd, sj):
    proxies = {'http': "http://10.10.100.215:80",'https': 'https://10.10.100.215:80'}
    headers = {}  # 自定义
    keyvalue = {}  # 传递参数
    url = 'http://data.stats.gov.cn/easyquery.htm'
    '''0 获得cookie'''
    driver = webdriver.PhantomJS(r'C:\Users\changchen\AppData\Local\Continuum\anaconda3\Lib\site-packages\selenium\webdriver\phantomjs')
    driver.get(url, proxies=proxies)
    cookie_list = driver.get_cookies(proxies=proxies)
    cookie_dict = {}
    for cookie in cookie_list:
        cookie_dict[cookie['name']] = cookie['value']
    ''' 1 填充头部'''
    headers[
        'User-Agent'] = 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
    headers['Cookie'] = "u_trs_uv=jw8l5czc_6_jh8a; u=%s; experience=show; JSESSIONID=%s" % (
    cookie_dict['u'], cookie_dict['JSESSIONID'])
    #     headers['Cookie'] = '_trs_uv=jw8l5czc_6_jh8a; u=5; experience=show; JSESSIONID=0CDCB8743DBA7F068231AB64E372B248; td_cookie=568593586'
    #     headers['Cookie'] = '_trs_uv=jw8l5czc_6_jh8a; u=5; experience=show; JSESSIONID=AEA7C0EE492DFD68B86519FF9585F44B;'
    ''' 2 下面是参数的填充 '''
    # keyvalue['id'] = 'A04'
    keyvalue['id'] = df_id
    keyvalue['m'] = 'QueryData'
    keyvalue['s'] = '固定资产投资完成额'
    keyvalue['dbcode'] = dbcode
    keyvalue['rowcode'] = 'zb'
    keyvalue['colcode'] = 'sj'
    keyvalue['wds'] = '[]'
    # keyvalue['dfwds'] = '[{"wdcode":"zb","valuecode":"A0401"}]'
    keyvalue['dfwds'] = '[{"wdcode":"zb","valuecode":"%s"}]' % (zd)
    keyvalue['k1'] = str(gettime())
    s = requests.session()
    keyvalue['dfwds'] = '[{"wdcode":"sj","valuecode":"%s"}]' % (sj)
    r = s.get(url, params=keyvalue, headers=headers, proxies=proxies)
    soup = BeautifulSoup(r.text, 'lxml')
    df = soup.p.text
    df = json.loads(df)
    data = []
    zb = []
    sj = []
    for i in df.get('returndata').get('datanodes'):
        m_data = i.get('data').get('data')
        #   print(m_data)
        data.append(m_data)
        b = i.get('wds')
        for bb in b:
            if bb['wdcode'] == 'zb':
                m_zb = bb.get('valuecode')
                zb.append(m_zb)
            elif bb['wdcode'] == 'sj':
                m_sj = bb.get('valuecode')
                sj.append(m_sj)
    df_zb_sj = pd.DataFrame({'zb': zb, 'sj': sj, 'data': data})

    '''4 获取指标和字段名字'''
    name = []
    code = []
    a1 = df.get('returndata').get('wdnodes')[0]
    a2 = df.get('returndata').get('wdnodes')[1]
    for a in a1['nodes']:
        name.append(a['cname'])
        code.append(a['code'])
    for b in a2['nodes']:
        name.append(b['cname'])
        code.append(b['code'])
    df_name_code = pd.DataFrame({'name': name, 'code': code})

    df_zb_sj_name = pd.merge(df_zb_sj, df_name_code, how='left', left_on='sj', right_on='code')
    df_zb_sj_name.rename(columns={'name': 'sj_name'}, inplace=True)
    del df_zb_sj_name['code']
    df_zb_sj_name = pd.merge(df_zb_sj_name, df_name_code, how='left', left_on='zb', right_on='code')
    df_zb_sj_name.rename(columns={'name': 'zb_name'}, inplace=True)
    del df_zb_sj_name['code']
    df_zb_sj_name['zb_name'].unique()
    return df_zb_sj_name

'''执行第一次 从19年开始执行 '''
def start_two():
    df_ppi = get_df('A0108', 'hgyd', 'A010801', '2019-')
    df_pmi_a = get_df('A0B', 'hgyd', 'A0B01', '2019-')
    df_pmi_b = get_df('A0B', 'hgyd', 'A0B02', '2019-')

    df_ppi['name'] = '工业生产者出厂价格指数PPI'
    df_pmi_a['name'] = '采购经理人指数PMI'
    df_pmi_b['name'] = '采购经理人指数PMI'
    ddd = [df_ppi, df_pmi_a, df_pmi_b]
    for df in ddd:
        if 'year' not in df.columns:
            df['year'] = [i[0:4] for i in df['sj']]
    df_all = pd.concat(ddd, axis=0, ignore_index=True)
    return df_all

def dels():
    sql_today_0 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='工业生产者出厂价格指数(上年同月=100)' and  更新时间  IN {0}""".format(df_today)
    sql_today_1 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='生产资料工业生产者出厂价格指数(上年同月=100)' and  更新时间  IN {0}""".format(df_today)
    sql_today_2 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='生活资料工业生产者出厂价格指数(上年同月=100)' and  更新时间  IN {0}""".format(df_today)
    sql_today_3 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='制造业采购经理指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_4 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='生产指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_5 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='新订单指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_6 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='新出口订单指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_7 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='在手订单指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_8 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='产成品库存指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_9 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='采购量指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_10 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='进口指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_11 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='出厂价格指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_12 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='主要原材料购进价格指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_13 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='原材料库存指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_14 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='从业人员指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_15 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='供应商配送时间指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_16 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='生产经营活动预期指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_17 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='非制造业商务活动指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_18 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='存货指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_19 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='投入品价格指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_20 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='销售价格指数' and  更新时间  IN {0}""".format(df_today)
    sql_today_21 = """ DELETE from pur_国家统计局_pmi_ppi WHERE zb_name='业务活动预期指数' and  更新时间  IN {0}""".format(df_today)
    del_sql(sql_today_0)
    del_sql(sql_today_1)
    del_sql(sql_today_2)
    del_sql(sql_today_3)
    del_sql(sql_today_4)
    del_sql(sql_today_5)
    del_sql(sql_today_6)
    del_sql(sql_today_7)
    del_sql(sql_today_8)
    del_sql(sql_today_9)
    del_sql(sql_today_10)
    del_sql(sql_today_11)
    del_sql(sql_today_12)
    del_sql(sql_today_13)
    del_sql(sql_today_14)
    del_sql(sql_today_15)
    del_sql(sql_today_16)
    del_sql(sql_today_17)
    del_sql(sql_today_18)
    del_sql(sql_today_19)
    del_sql(sql_today_20)
    del_sql(sql_today_21)
if __name__ == '__main__':
    df = start_two()
    print(df)
    # df_today = tuple(df['sj'].tolist())
    '''1 删除数据库中这些日期的数据'''
    # dels()
    # '''2 s数据存入数据库'''
    # sheet_name = 'pur_国家统计局_pmi_ppi'
    # to_sql(df, sheet_name)
