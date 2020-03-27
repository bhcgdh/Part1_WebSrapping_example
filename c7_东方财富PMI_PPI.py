#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''===============步骤2 放到环境里的代码 '''
import pandas as pd
import re
import requests, json, urllib
from bs4 import BeautifulSoup
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')
from a0_mysql import del_sql,get_sql_data,to_sql
# from a0_serversql import del_sql,get_sql_data,to_sql  # sql server 数据库

url_1 = 'https://data.eastmoney.com/cjsj/pmi.html'
url_2 = 'https://data.eastmoney.com/cjsj/ppi.html'
url_2s = 'https://data.eastmoney.com/cjsj/productpricesindex.aspx?p=2'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
proxies = {'http': "http://10.10.100.215:80", 'https': 'https://10.10.100.215:80'}

def get_soup(url, prox_con):
    if prox_con=='true':
        res = requests.get(url, headers=headers, proxies=proxies)
    else:
        res = requests.get(url, headers=headers)


    res.encoding = 'gb18030'
    # res.encoding = 'utf-8'
    req = res.text
    soup = BeautifulSoup(req, 'lxml')
    return soup

def get_data(soup):
    s0 = []
    a1 = soup.findAll('table')[0].findAll('tr')
    for i in a1[2:10]:
        a2 = i.stripped_strings
        s1 = []
        for j in a2:
            s1.append(j)
        s0.append(s1)
    return s0


def get_data2(soup):
    s0 = []
    a1 = soup.findAll('table')[0].findAll('tr')
    for i in a1[1:]:
        a2 = i.stripped_strings
        s1 = []
        for j in a2:
            s1.append(j)
        s0.append(s1)
    return s0


def get_PMI_PPI(url1, url2, url2s,prox_con):
    soup1 = get_soup(url1,prox_con)
    soup2 = get_soup(url2,prox_con)
    soup2s = get_soup(url2s,prox_con)

    df1 = get_data(soup1)
    df2 = get_data2(soup2)
    df2s = get_data2(soup2s)

    df1 = pd.DataFrame(df1, columns=['日期', '制造业_指数', '制造业_同比增长', '非制造业_指数', '非制造业_同比增长'])
    df2 = pd.DataFrame(df2, columns=['日期', '当月', '当月同比增长', '累计'])
    df2s = pd.DataFrame(df2s, columns=['日期', '当月', '当月同比增长', '累计'])
    df_p = pd.concat([df2, df2s])
    df_p = df_p[~(df_p['日期'] == "pageit('9');")]
    df_p.drop_duplicates(inplace=True)
    return df1, df_p

def get_ppi(df):
    df['日期'] = [i.replace('份', '1日') for i in df['日期']]
    df['sj'] = [i[0:4] + i[5:7] for i in df['日期']]
    df['zb'] = ''
    df['year'] = [i[0:4] for i in df['日期']]
    df['当月'] = df['当月'].astype('float')
    df['zb_name'] = '工业生产者出厂价格指数(上年同月=100)'
    df['name'] = '工业生产者出厂价格指数PPI'
    df['sj_yes'] = [str(int(i) - 100) for i in df['sj']]
    df.rename(columns=keys, inplace=True)
    dfa = pd.merge(df, df[['data', 'sj']], how='left', left_on='sj_yes', right_on='sj')
    dfa['同比增长_y'] = dfa['data_x'] / dfa['data_y'] - 1
    dfa['同比增长_y'] = dfa['同比增长_y'].apply(lambda x: format(x, '.2%'))
    del dfa['data_y']
    del dfa['sj_yes']
    del dfa['sj_y']
    del dfa['同比增长']
    keys2a = {'sj_x': 'sj',
              '同比增长_y': '同比增长',
              'data_x': 'data',
              }
    dfa.rename(columns=keys2a, inplace=True)
    dfa['sj'] = [str(i[0:4]) + '-' + str(i[5:7]) + '-' + str('01') for i in dfa['sj_name']]
    ''' 筛选出最大的年份数据'''
    dfa = dfa[dfa['year'] == df_ppi['year'].max()].reset_index(drop=True)
    return dfa

def get_pmi_two(df):
    df['日期'] = [i.replace('份', '1日') for i in df['日期']]
    df['sj'] = [i[0:4] + i[5:7] for i in df['日期']]
    df['zb'] = ''
    df['year'] = [i[0:4] for i in df['日期']]
    df['累计值'] = ''

    '''1 PMI-制造业--网站同比值可用'''
    dfa = df[['日期', '制造业_指数', '制造业_同比增长', 'sj', 'zb', 'year', '累计值']]
    dfa['name'] = '采购经理人指数PMI'
    dfa['zb_name'] = '制造业采购经理指数'
    dfa.rename(columns=keys, inplace=True)

    '''2 PMI-非制造业--网站同比值可用'''
    dfb = df[['日期', '非制造业_指数', '非制造业_同比增长', 'sj', 'zb', 'year', '累计值']]
    dfb['name'] = '采购经理人指数PMI'
    dfb['zb_name'] = '非制造业商务活动指数'
    dfb.rename(columns=keys, inplace=True)
    dfa['sj'] = [str(i[0:4]) + '-' + str(i[5:7]) + '-' + str('01') for i in dfa['sj_name']]
    dfb['sj'] = [str(i[0:4]) + '-' + str(i[5:7]) + '-' + str('01') for i in dfb['sj_name']]
    return dfa, dfb


if __name__ == '__main__':
    keys = {'日期': 'sj_name', '制造业_指数': 'data', '制造业_同比增长': '同比增长',
            '非制造业_指数': 'data', '非制造业_同比增长': '同比增长',
            '当月': 'data', '当月同比增长': '同比增长', '累计': '累计值'}
    prox_con = 'true' # 使用代理
    prox_con = 'false'# 不使用代理
    df_pmi, df_ppi = get_PMI_PPI(url_1, url_2, url_2s, prox_con)
    df3_pmi_z, df3_pmi_fz = get_pmi_two(df_pmi)  # 制造业和非制造业的数据
    df3_ppi_p = get_ppi(df_ppi)

    # df3_pmi_z['同比增长'] = [round(0.01*float(i.replace('%','')),6) for i in df3_pmi_z['同比增长']]
    # df3_pmi_fz['同比增长'] = [round(0.01*float(i.replace('%','')),6) for i in df3_pmi_fz['同比增长']]
    # df3_ppi_p['同比增长'] = [round(0.01*float(i.replace('%','')),6) for i in df3_ppi_p['同比增长']]
    #
    # df_today1 = tuple(df3_pmi_z['sj_name'].tolist())
    # df_today2 = tuple(df3_pmi_fz['sj_name'].tolist())
    # df_today3 = tuple(df3_ppi_p['sj_name'].tolist())
    #
    # sql_today1 = """ DELETE from pur_国家统计局_pmi_ppi WHERE  zb_name='制造业采购经理指数' AND pur_国家统计局_pmi_ppi.sj_name  IN {0}""".format(df_today1)
    # sql_today2 = """ DELETE from pur_国家统计局_pmi_ppi WHERE  zb_name='非制造业商务活动指数' AND pur_国家统计局_pmi_ppi.sj_name  IN {0}""".format(df_today2)
    # sql_today3 = """ DELETE from pur_国家统计局_pmi_ppi WHERE  name='工业生产者出厂价格指数PPI' AND pur_国家统计局_pmi_ppi.sj_name  IN {0}""".format(df_today3)
    #
    # del_sql(sql_today1)
    # del_sql(sql_today2)
    # del_sql(sql_today3)
    # sheet_name = 'pur_国家统计局_pmi_ppi'
    # to_sql(df3_pmi_z, sheet_name)
    # to_sql(df3_pmi_fz, sheet_name)
    # to_sql(df3_ppi_p, sheet_name)
