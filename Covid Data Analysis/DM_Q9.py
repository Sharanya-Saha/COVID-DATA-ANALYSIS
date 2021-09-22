#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  8 19:32:09 2021

@author: sharanya
"""

import pandas as pd
from datetime import timedelta, date

df=pd.read_csv('datasets/statewise_vaccinated.csv')
df['populationleft']=df['TOT_P']-df['dose1']

state_name_to_state_id={'andaman and nicobar islands':'AN','andhra pradesh':'AP','arunachal pradesh':'AR','assam':'AS','bihar':'BR','chandigarh':'CH','chhattisgarh':'CT','delhi':'DL','dadra and nagar haveli and daman and diu':'DN','goa':'GA','gujarat':'GJ','haryana':'HR','himachal pradesh':'HP','jammu and kashmir':'JK','jharkhand':'JK','karnataka':'KA','kerala':'KL','lakshadweep':'LD','madhya pradesh':'MP','maharashtra':'MH','manipur':'MN','meghalaya':'ML','mizoram':'MZ','nagaland':'NL','odisha':'OR','puducherry':'PY','punjab':'PB','rajasthan':'RJ','sikkim':'SK','tamil nadu':'TN','tripura':'TR','uttar pradesh':'UP','uttarakhand':'UT','west bengal':'WB'}

for state in state_name_to_state_id :
    df.loc[df.Name==state,'Name']=state_name_to_state_id[state]

df_week=pd.read_csv('Outputs/state-vaccinated-count-weekly.csv')
df_week_last=df_week[df_week['Week_id']==30]
df_week_last=df_week_last[df_week_last['State_Code']!='TG']
df_week_last=df_week_last[df_week_last['State_Code']!='LA'].reset_index()
df['rate(per week)']=df_week_last['dose1']
df['days required']=7*df['populationleft']/df['rate(per week)']
df['days required']=df['days required'].astype(int)


Begindate = date(2021,8,15)
#df['final_date'] = Begindate + timedelta(days=df['days required'])
date_list=list(df['days required'].values)

t=[]
for d in date_list :
    t.append(Begindate + timedelta(days=int(d)))
    
df['final_date']=t

df=df[['Name','populationleft','rate(per week)','final_date']]
df= df.rename(columns = {'Name': 'stateid', 'rate(per week)': 'rate','final_date':'date'}, inplace = False)
df.to_csv('Outputs/complete-vaccination.csv',index=False)
