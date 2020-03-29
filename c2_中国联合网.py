#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path
import os

s = Path(os.getcwd())
import requests
from bs4 import BeautifulSoup
import warnings

warnings.filterwarnings('ignore')
from selenium import webdriver
from lxml import html

from a0_mysql import del_sql,get_sql_data,to_sql
# from a0_serversql import del_sql,get_sql_data,to_sql  # sql server 数据库

'''2  可以获取除去2.4的其他数据，这里获取的是上月数据，为当天获取。非历史数据'''


def get_data(url, proxy_cont):
    session = requests.Session()
    proxies = {'http': "http://10.10.100.215:80", 'https': 'https://10.10.100.215:80'}

    if proxy_cont == 'true':
        session.post(login_url, headers=headers, data=data, proxies=proxies)
        res = session.get(url, headers=headers, proxies=proxies)
    else:
        session.post(login_url, headers=headers, data=data)
        res = session.get(url, headers=headers)

    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'lxml')
    return soup


def get_data2(url, proxy_cont):
    session = requests.Session()
    if proxy_cont == 'true':
        session.post(login_url, headers=headers, data=data, proxies=proxies)
        res = session.get(url, headers=headers, proxies=proxies)
    else:
        session.post(login_url, headers=headers, data=data)
        res = session.get(url, headers=headers)
    res.encoding = 'gb18030'
    soup = BeautifulSoup(res.text, 'lxml')
    return soup


'''3 数据转换'''


def trans_data(sou):
    s1 = sou.text
    s1 = s1.replace('null', '"null"')
    s2 = eval(s1.split('{"tabh":[')[1].split('],"data":[')[0])
    s3 = eval(s1.split('{"tabh":[')[1].split('],"data":[')[1][:-2])
    ''' 获取数据相互间信息'''
    cols = [list(i.keys()) for i in s3[:1]][0]
    data = []
    for i in s3:
        data.append(i.values())
    d = pd.DataFrame.from_dict(data)
    d.columns = cols
    return d


def get_1234(proxy_cont):
    soup1 = get_data(url_1, proxy_cont)
    soup2 = get_data(url_2, proxy_cont)
    soup3 = get_data(url_3, proxy_cont)
    soup5 = get_data(url_5, proxy_cont)
    columns_key = {'TIME': 'time', 'PRICE': '价格', 'PRICEPRO': '价格属性',
                   'UNIT': '单位', 'FIELD': '钢厂', 'AREA': '钢厂',
                   'MATERIAL': '材质', 'REMARK': '备注', 'STANDARD': '规格',
                   'TOWN': '城市', 'VARIETY': '品名', 'PRICE_UP': '涨跌'}
    df1 = trans_data(soup1)
    df2 = trans_data(soup2)
    df3 = trans_data(soup3)
    df5 = trans_data(soup5)

    del df1['LEV']
    del df2['LEV']
    del df3['LEV']
    del df3['REMARK']
    del df5['LEV']

    columns_key1 = {'TIME': 'time', 'PRICE': '价格', 'PRICEPRO': '价格属性',
                    'UNIT': '单位', 'FIELD': '钢厂', 'AREA': '钢厂',
                    'MATERIAL': '材质', 'REMARK': '备注', 'STANDARD': '规格',
                    'TOWN': '城市', 'VARIETY': '品名', 'PRICE_UP': '涨跌'}
    df1['title'] = ''
    df1['规格（mm）'] = ''

    columns_key2 = {'TIME': 'time', 'PRICE': '价格', 'PRICEPRO': '价格属性',
                    'UNIT': '单位', 'AREA': '产地',
                    'MATERIAL': '材质', 'REMARK': '备注', 'STANDARD': '规格',
                    'TOWN': '城市', 'VARIETY': '品名', 'PRICE_UP': '涨跌'}
    df2['title'] = ''
    df2['付款方式'] = ''

    columns_key3 = {'TIME': 'time', 'PRICE': '价格', 'PRICEPRO': '价格属性',
                    'UNIT': '单位', 'AREA': '产地',
                    'MATERIAL': '材质', 'REMARK': '备注', 'STANDARD': '规格(mm)',
                    'TOWN': '城市', 'VARIETY': '品名', 'PRICE_UP': '涨跌'}

    columns_key5 = {'TIME': 'time', 'PRICE': '价格', 'PRICEPRO': '价格属性',
                    'UNIT': '单位', 'FIELD': '钢厂', 'AREA': '钢厂',
                    'MATERIAL': '材质', 'REMARK': '备注', 'STANDARD': '规格（mm）',
                    'TOWN': '城市', 'VARIETY': '品名', 'PRICE_UP': '涨跌'}
    df5['title'] = ''
    df1.rename(columns=columns_key1, inplace=True)
    df2.rename(columns=columns_key2, inplace=True)
    df3.rename(columns=columns_key3, inplace=True)
    df5.rename(columns=columns_key5, inplace=True)
    return df1, df2, df3, df5


''' 所有的武汉历史数据，冷轧'''


def get_45(proxy_cont):
    p_page = 'http://www.custeel.com/reform/moreList.mv?page='
    p_4 = '&rows=&length=&group=1001&cat=1008004&area=&factory=1001003&topic=0&grade=&keyword=&year=0&month=0&day=0&isall=0&search=false'
    pages_4 = p_page + str(1) + p_4
    df_h1 = get_data2(pages_4, proxy_cont)
    history_url = ['http://www.custeel.com/reform/' + i.attrs['href'] for i in
                   df_h1.find_all(class_='acticlelist')[0].find_all('a')]
    url4 = history_url[0]
    u45 = get_data2(url4, proxy_cont)
    a1 = u45.find_all('table')[1].find_all('tr')
    h_title = u45.find_all(class_='blog-single-head-title')[0].text
    h_time = u45.find_all(class_='blog-single-head-date')[0].span.text
    col = ['品种', '牌号', 'φ12-13mm', 'φ14-15mm', 'φ16-18mm', 'Φ19-20mm', 'Φ21-49mm', 'Φ50-85mm', 'φ86-100mm', 'φ101-30mm',
           'φ131-150mm', 'φ151-180mm']
    for i, a in enumerate(a1):
        if a.td.text == '优碳钢':
            s = [i.text for i in a.find_all('td')]
            h_d2 = pd.DataFrame([s], columns=col)
            h_d2['title'] = h_title
            h_d2['time'] = h_time
    return h_d2


if __name__ == '__main__':
    proxy_cont = 'true'
    # proxy_cont = 'false'
    proxies = {'http': "http://10.10.100.215:80", 'https': 'https://10.10.100.215:80'}

    '''1 基本数据，headers和网站的账号密码，登录网址的网址 '''
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
    ''' 注意，最好需要注册一个新的，公司用的账号，此处为私人账号，为避免可能的问题，后期会更改或清理，目前只做测试'''
    data = {'username': '用户名', 'password': '密码'}
    login_url = 'http://www.custeel.com/reform/loginAjax.mv'

    '''2 需要获取数据的网址 '''
    '''2.1 钢板---武汉冷轧，冷板 冷板 1.0*1250*2500 DC01 宝钢青山价格  武汉市场武钢产冷板1.0～1.5*1250*2500网络价每月最后5个交易日平均价  '''
    url_1 = 'http://www.custeel.com/reform/getGangPiPriceMapTab.mv?group=1001004&cat=1006001&code=&town=004003001'

    '''2.2 硅钢---武汉国产无取向硅钢价格行情-武汉 无取向硅钢 0.5*1200*C 50WW600 。
           产地武钢 P50WW600=每月25号价格（如遇节假日顺延或者提前一日）；其他根据市场价'''
    url_2 = 'http://www.custeel.com/reform/getCityPriceMapTab.mv?group=1001012&town=004003001'

    '''2.3 上海热轧--中国联合钢网（http://www.custeel.com）每月第一个工作日。上海热轧Q235B、上海冷板ST12、上海冷卷SPCC价格  '''
    url_3 = 'http://www.custeel.com/reform/getCityPriceMapTab.mv?group=1001003&town=003001001'

    ''' 2.4  钢厂—南钢—南钢X月21日部分产品出厂价格表 中的45#电价格 表格形式，
              中国联合钢铁网（http://www.custeel.com）—钢厂—南钢—南钢X月21日部分产品出厂价格表 中的45#电价格'''
    url_4 = '''http://www.custeel.com/reform/factory.mv?group=1001&factory=1001003'''

    ''' 2.5  冷轧——华东地区—上海冷轧价格行情 ——2.0*1250*2500 ST12 和2.3重复了，上海冷板ST12、上海冷卷SPCC价格 '''
    url_5 = 'http://www.custeel.com/reform/getGangPiPriceMapTab.mv?group=1001004&cat=1006001&code=&town=003001001'

    df1, df2, df3, df5 = get_1234(proxy_cont)

    df1['品名'] = df1['品名'].str.replace("冷板", "冷轧钢板")
    df2 = df2[df2['城市'] != '备注'].reset_index(drop=True)

    df4 = get_45(proxy_cont)
    df_today1 = tuple(df1['time'].tolist())
    df_today2 = tuple(df2['time'].tolist())
    df_today3 = tuple(df3['time'].tolist())
    df_today5 = tuple(df5['time'].tolist())
    df_today4 = tuple(df4['time'].tolist())

    '''1 删除数据库中这些日期的数据'''
    sql_today1 = """ DELETE from pur_联合钢1_武汉冷板 WHERE pur_联合钢1_武汉冷板.time IN {0}""".format(df_today1)
    sql_today2 = """ DELETE from pur_联合钢2_武汉硅钢 WHERE pur_联合钢2_武汉硅钢.time IN {0}""".format(df_today2)
    sql_today3 = """ DELETE from pur_联合钢3_上海热轧 WHERE pur_联合钢3_上海热轧.time IN {0}""".format(df_today3)
    sql_today5 = """ DELETE from pur_联合钢5_上海冷板 WHERE pur_联合钢5_上海冷板.time IN {0}""".format(df_today5)

    if len(df4['time'].unique().tolist()) == 1:
        df_today4 = df4['time'].unique().tolist()[0]
        sql_today4 = """ DELETE from pur_联合钢4_武汉无取向硅钢 WHERE  pur_联合钢4_武汉无取向硅钢.time = '{0}'""".format(df_today4)
    else:
        sql_today4 = """ DELETE from pur_联合钢4_武汉无取向硅钢 WHERE  pur_联合钢4_武汉无取向硅钢.time  IN {0}""".format(df_today4)

    del_sql(sql_today1)
    del_sql(sql_today2)
    del_sql(sql_today3)
    del_sql(sql_today5)
    del_sql(sql_today4)

    '''2 s数据存入数据库'''
    sheet_name1 = 'pur_联合钢1_武汉冷板'
    sheet_name2 = 'pur_联合钢2_武汉硅钢'
    sheet_name3 = 'pur_联合钢3_上海热轧'
    sheet_name5 = 'pur_联合钢5_上海冷板'
    sheet_name4 = 'pur_联合钢4_武汉无取向硅钢'
    to_sql(df1, sheet_name1)
    to_sql(df2, sheet_name2)
    to_sql(df3, sheet_name3)
    to_sql(df5, sheet_name5)
    to_sql(df4, sheet_name4)
