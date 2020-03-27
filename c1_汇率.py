# coding: utf-8
from pathlib import Path
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')
import os
from a0_mysql import del_sql,get_sql_data,to_sql

'''========================================================================================================================================
每天执行， 获取数据'''
def get_data(url, headers):
    proxies = {'http': "http://10.10.100.215:80",
               'https': 'https://10.10.100.215:80'}
    req = requests.get(url=url, headers=headers, proxies=proxies)
    # tex = req.text
    soup = BeautifulSoup(req.text, 'lxml')
    ''' 只是美元和中元兑换？'''
    body = soup.find_all(class_='genTbl closedTbl historicalTbl')[0].find_all('tr')
    s = []
    for  j  in range(len(body)):
        a = [i for i in body[j].text.split('\n')  if i!='']
        s.append(a)
    data = pd.DataFrame(s[1:],columns=s[0])
    return data
# 更改时间格式
def trans(a):
    a = a.replace('年', "-")
    a = a.replace('月', "-")
    a = a.replace('日', "")
    a1 = a.split('-')[0]
    a2 = a.split('-')[1]
    a3 = a.split('-')[2]

    if len(str(a2)) < 2:
        a2 = '0' + str(a2)
    else:
        a2 = str(a2)

    if len(str(a3)) < 2:
        a3 = '0' + str(a3)
    else:
        a3 = str(a3)
    a = a1 + '-' + a2 + '-' + a3
    return a
'''============================= myslq 语句集合================================================================
sql2：  删除数据库中已有的今天爬取的数据
sql3：  今天爬取的数据存入数据库
'''

def history_(sheet_name):
    '''历史数据，手动下载了2017年至2019年10月21号的数据，传入数据库中'''
    f1 = 'E:/cc/采购/Data_histroy'
    f2 = [f1+'/'+i for i in os.listdir(f1) if '历史数据'in i]
    f3 = [i.split('/')[-1].split('历史数据')[0] for i in f2]
    for i, fil in enumerate(f2):
        print(fil)
        file = open(fil, encoding='utf-8')
        df = pd.read_csv(file)
        df['币种'] = f3[i]
        to_sql(df,sheet_name)  #

def today(headers, sheet_name):
    url_usd = 'https://cn.investing.com/currencies/usd-cny-historical-data'  # 美元
    url_gbp = 'https://cn.investing.com/currencies/gbp-cny-historical-data'  # 英镑
    url_eur = 'https://cn.investing.com/currencies/eur-cny-historical-data'  # 欧元

    df_usd  = get_data(url_usd, headers)
    df_gbp = get_data(url_gbp, headers)
    df_eur = get_data(url_eur, headers)

    df_usd['币种'] = 'USD_CNY'
    df_gbp['币种'] = 'GBP_CNY'
    df_eur['币种'] = 'EUR_CNY'

    df_usd['日期'] = [trans(i) for i in df_usd['日期']]
    df_gbp['日期'] = [trans(i) for i in df_gbp['日期']]
    df_eur['日期'] = [trans(i) for i in df_eur['日期']]

    df_usd['涨跌幅'] = [round(0.01 * float(i.replace('%', '')), 6) for i in df_usd['涨跌幅']]
    df_gbp['涨跌幅'] = [round(0.01 * float(i.replace('%', '')), 6) for i in df_gbp['涨跌幅']]
    df_eur['涨跌幅'] = [round(0.01 * float(i.replace('%', '')), 6) for i in df_eur['涨跌幅']]

    today_usd = tuple(df_usd['日期'].unique().tolist())
    today_gbp = tuple(df_gbp['日期'].unique().tolist())
    today_eur = tuple(df_eur['日期'].unique().tolist())

    '''1 删除数据库中这些日期的数据'''
    sql2_usd = """ DELETE from pur_汇率 WHERE  币种 = 'USD_CNY'and 日期  IN {0}""".format(today_usd)
    sql2_gbp = """ DELETE from pur_汇率 WHERE  币种 = 'GBP_CNY'and 日期  IN {0}""".format(today_gbp)
    sql2_eur = """ DELETE from pur_汇率 WHERE  币种 = 'EUR_CNY'and 日期  IN {0}""".format(today_eur)
    del_sql(sql2_usd)
    del_sql(sql2_gbp)
    del_sql(sql2_eur)

    '''2 数据存入'''
    to_sql(df_usd,sheet_name)
    to_sql(df_gbp,sheet_name)
    to_sql(df_eur,sheet_name)


if __name__ == '__main__':
    sheet_name = 'pur_汇率'
    '''  第一次执行， 删除已有同名的表,创建汇率表 '''
    '''  存储本地历史数据，本地下载传入数据库中，下载的为20170101-20191021'''
    # history_(sheet_name)
    # '''每天凌晨1点执行一次，更新前一天的数据   每天当天12点执行一次，作为当天的数据'''
    head ={'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
    today(head,'pur_汇率')

