
# coding: utf-8




'''2 ======获取上海有色的数据 ==================================================================='''
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import warnings
warnings.filterwarnings("ignore")
import time
from a0_mysql import del_sql,get_sql_data,to_sql
# from a0_serversql import del_sql,get_sql_data,to_sql  # sql server 数据库

def deal_str(a):
    if type(a)==list:
        if len(a)>0:
            return a[0]
        else:
            return None
    else :
        return a

def get():
    url_smm = 'https://www.smm.cn/'
    # proxies = {'http': "http://10.10.100.215:80",
    #            'https': 'https://10.10.100.215:80'}
    # headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
    # r = requests.get(url=url_smm, headers=headers, proxies=proxies)

    headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
    r = requests.get(url=url_smm, headers=headers)
    html = r.text
    soup = bs(html, 'html.parser')
    soup = bs(html, 'html.parser')
    a = soup.find_all('tr')
    s = []
    for i in a:
        b = i.find_all('td')
        b0 = []
        b1 = []
        b2 = []
        b3 = []
        b4 = []
        b5 = []
        for n,j in enumerate(b):
            for k in j.stripped_strings:
                if n == 0:
                    b0.append(k)
                elif n==1:
                    b1.append(k)
                elif n==2:
                    b2.append(k)
                elif n==3:
                    b3.append(k)
                elif n==4:
                    b4.append(k)
                else:
                    b5.append(k)

        s1 = [b0[0], b1,b2,b3,b4,b5]
        s.append(s1)

    df = pd.DataFrame(s[1:])
    df.columns = [deal_str(i) for i in s[0]]
    for col in  df.columns[1:]:
        df[col] = [deal_str(i) for i in df[col]]
    df.columns = ['产品名称', '价格区间', '均价', '涨跌','单位', '更新日期']
    df = df[['产品名称', '价格区间', '均价', '涨跌','更新日期']]
    return df

''' 获取锌数据'''
def get_xin():
    url2 = 'https://hq.smm.cn/xin'
    # proxies = {'http': "http://10.10.100.215:80",
    #            'https': 'https://10.10.100.215:80'}
    # headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
    # r = requests.get(url=url2, headers=headers, proxies=proxies)
    #
    headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
    r = requests.get(url=url2, headers=headers)
    html = r.text
    soup = bs(html,'lxml')
    data = soup.find_all('tr')
    df = [i for i in data[2].stripped_strings if i!='仓库自提指导价' and i!='元/吨']
    df = pd.DataFrame([df],columns=['产品名称','价格区间','均价','涨跌','更新日期'])
    return  df

def get_df():
    df = get()
    df_xin = get_xin()
    dfs = pd.concat([df,df_xin],axis=0)
    df2 = dfs[(dfs['产品名称'].str.contains('1#电解铜'))|
         (dfs['产品名称'].str.contains('1#锌锭'))|
         (dfs['产品名称']=='SMM A00铝')|
         (dfs['产品名称'].str.contains('1#电解镍'))|
         (dfs['产品名称'].str.contains('1#铅锭'))|
         (dfs['产品名称'].str.contains('1#锡'))].reset_index(drop=True)
    dyear = time.localtime().tm_year
    df2['更新日期'] = [str(dyear) + '-' + i for i in df2['更新日期']]
    return df2

if __name__ == '__main__':
    ''' 0 显示要获取的数据：'''
    dfs = get_df()
    smm = {"SMM 1#电解铜": "1#电解铜",
           "SMM 1#锌锭": "1#锌锭",
           "SMM A00铝": "Aoo铝锭",
           "SMM 1#电解镍": "1#电解镍",
           "SMM 1#铅锭": "1#铅锭",
           "SMM 1#锡": "1#锡锭"}
    dfs.replace({"产品名称": smm}, inplace=True)
    df_today = tuple(dfs['更新日期'].unique().tolist())
    print(df_today)

    '''1 删除数据库中这些日期的数据'''
    if len(dfs['更新日期'].unique().tolist())==1:
        df_today1 = dfs['更新日期'].unique().tolist()[0]
        sql_today = """ DELETE from pur_上海有色 WHERE 更新日期 = '{0}'""".format(df_today1)
    else:
        sql_today = """ DELETE from pur_上海有色 WHERE 更新日期  IN {0}""".format(df_today)
    del_sql(sql_today)
    '''2 数据存入'''
    sheet_name = 'pur_上海有色'

    dfs = dfs[(dfs.产品名称 != 'SMM 广东1#电解铜')&(dfs.产品名称 != 'SMM鹰潭1#电解铜')]
    to_sql(dfs, sheet_name)





