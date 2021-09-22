#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 23:33:06 2021

@author: sharanya
"""

import pandas as pd
from datetime import timedelta, date


def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

df=pd.read_csv('datasets/districts.csv',low_memory=False)
df=df[['Date','State','District','Confirmed']].copy()

# Used for district and district_key mapping
district_code_df=pd.read_csv('datasets/district_wise.csv',low_memory=False)
district_code_df=district_code_df[['District_Key','District']]
district_code_dict=dict(zip(list(district_code_df['District'].values),list(district_code_df['District_Key'].values)))

#Only considering data from 15th March 2020 to 14th August 2021
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
df = df[df['Date']<"2021/08/15"]


remove=['Unknown','Evacuees','Railway Quarantine','State Pool','Foreign Evacuees','Italians','Airport Quarantine','Other State','Other Region','Others','Capital Complex','Unassigned']

df = df.loc[df['District']!='BSF Camp']
for remove_d in remove :
    df = df.loc[df['District']!=remove_d]

#t = set(list(df['District'].values))-set(list(district_code_df['District'].values))
#t1= set(list(district_code_df['District'].values))-set(list(df['District'].values))

# Delhi is being replaced by district code as 'DL_Delhi' i.e All the Delhi are merged to a single Delhi
df['District'].replace('Delhi','DL_Delhi',inplace=True)

# For Handling the districts with same names
df.loc[(df.State == "Himachal Pradesh") & (df.District == 'Hamirpur'), "District"] = "HP_Hamirpur"
df.loc[(df.State == "Uttar Pradesh") & (df.District == 'Hamirpur'), "District"] = "UP_Hamirpur"
df.loc[(df.State == "Himachal Pradesh") & (df.District == 'Bilaspur'), "District"] = "HP_Bilaspur"
df.loc[(df.State == "Chhattisgarh") & (df.District == 'Bilaspur'), "District"] = "CT_Bilaspur"
df.loc[(df.State == "Rajasthan") & (df.District == 'Pratapgarh'), "District"] = "RJ_Pratapgarh"
df.loc[(df.State == "Uttar Pradesh") & (df.District == 'Pratapgarh'), "District"] = "UP_Pratapgarh"
df.loc[(df.State == 'Chattisgarh') & (df.District=='Bijapur'), 'District'] = 'CT_Bijapur'
df.loc[(df.State == 'Karnataka') & (df.District=='Bijapur'), 'District'] = 'KA_Bijapur'
df.loc[(df.State == 'Bihar') & (df.District == 'Aurangabad'),'District'] = 'BR_Aurangabad'
df.loc[(df.State == 'Maharashtra') & (df.District == 'Aurangabad'),'District'] = 'MH_Aurangabad'
df.loc[(df.State == 'Chattisgarh') & (df.District=='Balrampur'), 'District'] = 'CT_Balrampur'
df.loc[(df.State == 'Uttar Pradesh') & (df.District=='Balrampur'), 'District'] = 'UP_Balrampur'


#Replacing district names with district keys
for key in district_code_dict:
    df['District'].replace(key,district_code_dict[key],inplace=True)
    
#p = set(list(df['District'].values))-set(list(district_code_df['District_Key'].values))
df=df.sort_values(['District','Date'],ascending=[True,True])

#-----------------------------------------------------------------------------------------------------------
#overall cases District Wise
overall_df = pd.DataFrame({'Cases' : df.groupby('District').max()['Confirmed']}).reset_index()
overall_df['Overall_id'] = 1
overall_df = overall_df[["District", "Overall_id", "Cases"]]
#Exporting to CSV
overall_df.to_csv('Outputs/cases-overall.csv', index = False)
#print('cases-overall.csv is created')

#-----------------------------------------------------------------------------------------------------------
#correcting cumulative numbers to non-cumulative numbers
df.loc[df['District'] == df['District'].shift(), 'newnumber'] = (df.Confirmed - df.Confirmed.shift())
df.loc[df.newnumber.isna(),'newnumber'] = df.Confirmed
df['Confirmed'] = df['newnumber']
df.drop("newnumber", axis =1, inplace = True)


#-----------------------------------------------------------------------------------------------------------------
#District Wise weekly cases

def update_week(df_weekly1,weekid) :
    df_weekly=df_weekly1.copy()
    l1=list([weekid]*df_weekly1.shape[0])
    df_weekly['Week_id']= l1
    df_weekly = pd.DataFrame({'Cases' : df_weekly.groupby(['Week_id','District']).sum()['Confirmed']}).reset_index()
    df_weekly = df_weekly[['District', 'Week_id', 'Cases']]
    return df_weekly

df_weeklys=df[(df['Date']>='2020/04/26') & (df['Date']<'2020/05/03')]
#updated_df=update_week(df_weeklys,1)
weekid=0
df_weekly=df_weeklys.copy()
l1=list([weekid]*df_weeklys.shape[0])
df_weekly.insert(0,'Week_id',l1,True)
df_weekly = pd.DataFrame({'Cases' : df_weekly.groupby(['Week_id','District']).sum()['Confirmed']}).reset_index()
df_weekly = df_weekly[['District', 'Week_id', 'Cases']]
weekly_final=df_weekly


#Overlapping weeks are considered as this csv will be further used in Q4
start_dt = date(2020, 4, 26)
end_dt = date(2021, 8, 15)
t=[]
for dt in daterange(start_dt, end_dt):
    t.append(dt.strftime("%Y-%m-%d"))
 
weekid=0
flag=0
i=0
j=i+7
while i < len(t) and j < len(t):
    weekid=weekid+1
    #print(weekid,t[i],t[j-1])
    df_weekly=df[(df['Date']>=t[i]) & (df['Date']<t[j])]
    updated_df=update_week(df_weekly,weekid)
    weekly_final=pd.concat([weekly_final,updated_df],ignore_index=True)
    if flag == 0 :
        i=j-3
        flag=1
    else :
        i=j-4
        flag=0
    j=i+7


weekly_final = weekly_final.loc[weekly_final['Week_id']!=0]
weekly_final.sort_values(by=['District','Week_id'], inplace = True)

weekly_final.to_csv('Outputs/cases-week.csv', index = False) 
#print('cases-week.csv is created')


#--------------------------------------------------------------------------------------------------------------
#District wise monthly cases

def update_month(df_monthly,monthid) :
    df_month=df_monthly.copy()
    df_month['Month_id']=monthid
    df_month = pd.DataFrame({'Cases' : df_month.groupby(['Month_id','District']).sum()['Confirmed']}).reset_index()
    df_month = df_month[['District', 'Month_id', 'Cases']]
    return df_month
      
start_date= date(2020,4,15)
end_date=date(2021,8,15)
p=[]
for dt in daterange(start_date, end_date):
    p.append(dt.strftime("%Y-%m-%d"))


q=[]

i=0
for i in range(len(p)) :
    x=p[i].split('-')[2]
    #print(x)
    if x == '15' :
        q.append(p[i])
    elif x== '14' :
        q.append(p[i])

#print(q)
i=0
firstcal=1
monthid=0
while i < len(q)-1 :
    monthid=monthid+1
    df_monthly=df[(df['Date']>=q[i]) & (df['Date']<=q[i+1])]
    updated_monthly_df=update_month(df_monthly,monthid)
    if firstcal == 1 :
        monthly_final=updated_monthly_df
        firstcal=0
    else :
       monthly_final=pd.concat([monthly_final,updated_monthly_df],ignore_index=True) 
    i=i+2


monthly_final.sort_values(by=['District','Month_id'], inplace = True)
monthly_final.to_csv('Outputs/cases-month.csv',index=False)
#print('cases-month.csv is created')










