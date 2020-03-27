#!/usr/bin/env python
# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')
import requests
from bs4 import BeautifulSoup
import pandas as pd
from a0_mysql import del_sql,get_sql_data,to_sql
# from a0_serversql import del_sql,get_sql_data,to_sql  # sql server 数据库

'''1 基本数据，headers和网站的账号密码，登录网址的网址 '''
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
proxies = {'http': "http://10.10.100.215:80",
           'https': 'https://10.10.100.215:80'}

''' 注意，使用的是提供的账号密码 '''
data = {'txtUserName':'pzf88',
        'txtPassWord':'20071112',
       }
data2={
        'Head1$txtUserName': 'pzf88',
        'Head1$txtPassWord': '20071112',
        'Head1$hidDataType': '价格数据库',
        'Head1$hidUrl': 'http://www.chinaccm.cn/pricedata/PriceData.aspx',
        'begindate': '2019-09-11',
        'enddate': '2019-10-08',
        'txtCustom': '氧化镝+国内价格',
        'drpPageSize': "20",
        'HiddenField1': "12019-09-11%2422019-10-17%24",
        'hidcdtion': "12019-09-11$22019-10-17$",
        'hidDataType': "DATA_NATIONALPRICE",
        'hidDataTypeTitle': "国内价格",
        'hidMyCheckSuppName': "氧化镝",
        'hidFristSupCode': "28010101",
        'hidSort': "Date",
        'hidSCode': "28120207",
        'hidCata': "1",
        'hidMyCheckSuppCode': "2812",
        'hidCheckPriceData': "铁合金",
        'hidExistsDate': "true",
        'hidMainCustID': "pzf88",
        'hidCustID': "pzf88",
        'hidIp': "False",
        'hidDateLimit': "false",}

login_url = 'http://www.chinaccm.cn/MemberCenter/Login.aspx'
''' 默认只有最近7天的数据'''
url_Dy2O3 = 'http://www.chinaccm.cn/pricedata/PriceData.aspx?scode=28120207'

'''登录的url以及获取数据的url地址'''
def get_new(headers,data,login_url,url,proxies):
    session = requests.Session()
    session.post(login_url, headers=headers, data=data, proxies=proxies)
    res = session.get(url,headers=headers, data=data2, proxies=proxies)
    res.encodign='gb18030'
    soup = BeautifulSoup(res.text, 'lxml')
    ''' 解析数据'''
    dfs = []
    col = []
    for i in soup.find_all(class_='dmain_right_tab_title'):
        if '日期' in i.text:
            a = [j.text for j in i.find_all('th')]
            col = a
    for i in soup.find_all(class_='dmain_right_tab_list'):
        if '氧化镝' in i.text:
            a = [j.text for j in i.find_all('td')]
            dfs.append(a)
    # f = open(r'F:\\Learn\\python\\pycodes\\zhengt\\10_pa\\采购\\Data\\中华商务_氧化镝.csv',encoding='utf-8')
    # his = pd.read_csv(f)
    # his.columns
    col_finall = ['日期', '产品名称', '规格', '城市', '最低价', '最高价', '均价', '均价涨跌', '价格单位', '价格说明','价格类型']
    df = pd.DataFrame(dfs, columns=col)
    df.drop_duplicates(inplace=True)  #　去掉重复的内容
    df = df[col_finall]
    return df

if __name__ == '__main__':
    df = get_new(headers,data,login_url,url_Dy2O3,proxies)
    df = df[~df['日期'].str.contains('氧化镝')].reset_index(drop=True)
    df_today = tuple(df['日期'].tolist())
    '''1 删除数据库中这些日期的数据'''
    sql_today = """ DELETE from pur_中华商务1_氧化镝 WHERE 日期  IN {0}""".format(df_today)
    del_sql(sql_today)
    '''2 s数据存入数据库'''
    sheet_name = 'pur_中华商务1_氧化镝'
    to_sql(df, sheet_name)


