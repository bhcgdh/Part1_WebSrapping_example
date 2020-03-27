# coding: utf-8
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import warnings
warnings.filterwarnings("ignore")
from a0_mysql import del_sql,get_sql_data,to_sql
# from a0_serversql import del_sql,get_sql_data,to_sql  # sql server 数据库

'''1 ======获取白银数据 ==================================================================='''
def get_silver(url, headers, proxy_con):
    proxies = {'http': "http://10.10.100.215:80",
               'https': 'https://10.10.100.215:80'}
    try:
        if proxy_con=='true':
            r = requests.get(url=url, headers=headers, proxies=proxies)
        else:
            r = requests.get(url=url, headers=headers)
    except:
        print('网络连接失败')
    html = r.text
    soup = bs(html,'lxml')
    data = soup.find_all('tr')
    columns = data[0].find_all('th')
    col = [columns[i].contents[0] for i in range(len(columns))]
    df = pd.DataFrame()
    for i, tds in enumerate(data[1:]):
        td = tds.find_all('td')
        s = [td[0].contents[1].contents[0],td[1].contents[0],td[2].contents[0],td[3].contents[0],td[4].contents[0],td[5].contents[0]]
        df = df.append([s])
    df.columns = col
    return df

def down_silver_data(proxy_con):
    url = 'https://www.ebaiyin.com/hangqing/list_ht.shtml'
    headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
    try:
        df_01_silver = get_silver(url,headers,proxy_con)
    except:
        print('白银数据爬取失败')
    return df_01_silver

if __name__ == '__main__':
    proxy_con = 'true'  #  使用代理
    # proxy_con = 'false' #  未使用
    df = down_silver_data(proxy_con)
    df.columns = ["名称","规格","定盘价","结算价","单位","更新时间"]
    # 原始的时间是到小时分钟，这里处理到日，更新日数据
    df['更新时间'] = [i.split(' ')[0] for i in df['更新时间']]
    df = df[df['规格'] == '国标一号']
    df['规格'] = '1#白银'
    df = df[~(df['定盘价'].isna())]
    df_today =  df['更新时间'].tolist()[0]

    '''1 删除数据库中这些日期的数据'''
    sql_today = """ DELETE from pur_白银 WHERE 更新时间 in (' """ + df_today + """ ')"""
    del_sql(sql_today)
    '''2 s数据存入数据库'''
    sheet_name = 'pur_白银'
    to_sql(df, sheet_name)



