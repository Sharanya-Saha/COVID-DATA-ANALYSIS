#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 21:03:07 2021

@author: sharanya
"""

import pandas as pd
from datetime import timedelta, date



df=pd.read_csv('datasets/cowin_vaccine_data_districtwise.csv',low_memory=False)
df=df.drop(0)
df.reset_index()
districts=df['District_Key']
districts_valuecount=districts.value_counts()
districts_repeated=districts_valuecount[districts_valuecount>1].index
districts=list(set(districts))

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

start_dt = date(2021, 1, 16)
end_dt = date(2021, 8, 14)
dates=[]
for dt in daterange(start_dt, end_dt):
    dates.append(dt.strftime("%d/%m/%Y"))
    
required=[]
for j in range(len(dates)):
    for i in range(1,len(df)+1):
        req = {}
        req['State_Code'] = df.loc[i, 'State_Code']
        req['District_Key'] = df.loc[i, 'District_Key']
        req['District']=df.loc[i,'District']
        req['Date'] = dates[j]
        req['dose1'] = df.loc[i,dates[j]+'.3']
        req['dose2'] = df.loc[i,dates[j]+'.4']
        required.append(req)

dfr=pd.DataFrame(required)
dfr['Date'] = pd.to_datetime(dfr['Date'], format='%d/%m/%Y')
dfr=dfr.sort_values(['District_Key','Date'])


#debug=set(districts)-set(t)
dfr['dose1'].fillna(0,inplace=True)
dfr['dose2'].fillna(0,inplace=True)
dfr['dose1']=dfr['dose1'].astype(int)
dfr['dose2']=dfr['dose2'].astype(int)



# For districts with different names and same district key
dfmerged_dose1=pd.DataFrame({'dose1':dfr.groupby(['District_Key','Date'])['dose1'].sum()}).reset_index()
dfmerged_dose2=pd.DataFrame({'dose2':dfr.groupby(['District_Key','Date'])['dose2'].sum()}).reset_index()

#------------------------------------------------------------------------------------------------------------------

# District Wise overall vaccination
district_overall_d1=pd.DataFrame({'dose1':dfmerged_dose1.groupby(['District_Key'])['dose1'].max()}).reset_index()
district_overall_d2=pd.DataFrame({'dose2':dfmerged_dose2.groupby(['District_Key'])['dose2'].max()}).reset_index()

f_district_overall=district_overall_d1.copy()
f_district_overall['Overall_id']=1
f_district_overall['dose2']=district_overall_d2['dose2']
f_district_overall=f_district_overall[['District_Key','Overall_id','dose1','dose2']]
f_district_overall.to_csv('Outputs/district-vaccinated-count-overall.csv',index=False)
#print('district-vaccinated-count-overall.csv created')


#----------------------------------------------------------------------------------------------------------------

#correcting cumulative data for dose 1 to non cumulative
dfmerged_dose1.loc[dfmerged_dose1['District_Key'] == dfmerged_dose1['District_Key'].shift(), 'newnumber'] = (dfmerged_dose1.dose1 - dfmerged_dose1.dose1.shift())
dfmerged_dose1.loc[dfmerged_dose1.newnumber.isna(),'newnumber'] = dfmerged_dose1.dose1
dfmerged_dose1['dose1'] = dfmerged_dose1['newnumber']
dfmerged_dose1.drop("newnumber", axis =1, inplace = True)
dfmerged_dose1.loc[dfmerged_dose1['dose1']<0,'dose1']=0




#correcting cumulative data for dose 2 to non cumulative
dfmerged_dose2.loc[dfmerged_dose2['District_Key'] == dfmerged_dose2['District_Key'].shift(), 'newnumber'] = (dfmerged_dose2.dose2 - dfmerged_dose2.dose2.shift())
dfmerged_dose2.loc[dfmerged_dose2.newnumber.isna(),'newnumber'] = dfmerged_dose2.dose2
dfmerged_dose2['dose2'] = dfmerged_dose2['newnumber']
dfmerged_dose2.drop("newnumber", axis =1, inplace = True)
dfmerged_dose2.loc[dfmerged_dose2['dose2']<0,'dose2']=0

#----------------------------------------------------------------------------------------------------------------
#District Wise Monthly


def update_month1(df_monthly1,monthid) :
    df_month1=df_monthly1.copy()
    df_month1['Month_id']=monthid
    df_month1 = pd.DataFrame({'dose1' : df_month1.groupby(['Month_id','District_Key']).sum()['dose1']}).reset_index()
    df_month1 = df_month1[['District_Key', 'Month_id', 'dose1']]
    return df_month1

def update_month2(df_monthly2,monthid) :
    df_month2=df_monthly2.copy()
    df_month2['Month_id']=monthid
    df_month2 = pd.DataFrame({'dose2' : df_month2.groupby(['Month_id','District_Key']).sum()['dose2']}).reset_index()
    df_month2 = df_month2[['District_Key', 'Month_id', 'dose2']]
    return df_month2

month_dose1=dfmerged_dose1.copy()
month_dose2=dfmerged_dose2.copy()

start_date= date(2021,1,15)
end_date=date(2021,8,14)
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
    df_monthly1=month_dose1[(month_dose1['Date']>=q[i]) & (month_dose1['Date']<=q[i+1])]
    updated_monthly_df1=update_month1(df_monthly1,monthid)
    if firstcal == 1 :
        monthly_final1=updated_monthly_df1
        firstcal=0
    else :
       monthly_final1=pd.concat([monthly_final1,updated_monthly_df1],ignore_index=True) 
    i=i+2

monthly_final1.sort_values(by=['District_Key','Month_id'],inplace=True)

i=0
firstcal=1
monthid=0
while i < len(q)-1 :
    monthid=monthid+1
    df_monthly2=month_dose2[(month_dose2['Date']>=q[i]) & (month_dose2['Date']<=q[i+1])]
    updated_monthly_df2=update_month2(df_monthly2,monthid)
    if firstcal == 1 :
        monthly_final2=updated_monthly_df2
        firstcal=0
    else :
       monthly_final2=pd.concat([monthly_final2,updated_monthly_df2],ignore_index=True) 
    i=i+2

monthly_final2.sort_values(by=['District_Key','Month_id'],inplace=True)
monthly_final1['dose2']=monthly_final2['dose2']
monthly_final1.to_csv('Outputs/district-vaccinated-count-monthly.csv',index=False)
#print('district-vaccinated-monthly.csv created')

#------------------------------------------------------------------------------------------------
#District Wise Weekly

def update_week1(df_weekly1,weekid) :
    df_weekly=df_weekly1.copy()
    l1=list([weekid]*df_weekly1.shape[0])
    df_weekly['Week_id']= l1
    df_weekly = pd.DataFrame({'dose1' : df_weekly.groupby(['Week_id','District_Key']).sum()['dose1']}).reset_index()
    df_weekly = df_weekly[['District_Key', 'Week_id', 'dose1']]
    return df_weekly


def update_week2(df_weekly2,weekid) :
    df_weekly=df_weekly2.copy()
    l1=list([weekid]*df_weekly2.shape[0])
    df_weekly['Week_id']= l1
    df_weekly = pd.DataFrame({'dose2' : df_weekly.groupby(['Week_id','District_Key']).sum()['dose2']}).reset_index()
    df_weekly = df_weekly[['District_Key', 'Week_id', 'dose2']]
    return df_weekly

# For first week1 dose 1 and dose 2. First week is considered from 16/01/2020 to 23/01/2020

df_week1_district_d1=dfmerged_dose1[(dfmerged_dose1['Date']>='2021/01/16')&(dfmerged_dose1['Date']<='2021/01/23')]
df_week1_d1=df_week1_district_d1.copy()
df_week1_d1['Week_id']=1
df_week1_d1 = pd.DataFrame({'dose1' : df_week1_d1.groupby(['Week_id','District_Key']).sum()['dose1']}).reset_index()
df_week1_d1 = df_week1_d1[['District_Key', 'Week_id', 'dose1']]

df_week1_district_d2=dfmerged_dose2[(dfmerged_dose2['Date']>='2021/01/16')&(dfmerged_dose2['Date']<='2021/01/23')]
df_week1_d2=df_week1_district_d2.copy()
df_week1_d2['Week_id']=1
df_week1_d2 = pd.DataFrame({'dose2' : df_week1_d2.groupby(['Week_id','District_Key']).sum()['dose2']}).reset_index()
df_week1_d2 = df_week1_d2[['District_Key', 'Week_id', 'dose2']]
df_week1_d1['dose2']=df_week1_d2['dose2']
df_weekly_district=df_week1_d1


start_date= date(2021,1,24)
end_date= date(2021,8,14)

date_list=[]
for dt in daterange(start_date, end_date):
    date_list.append(dt.strftime("%Y-%m-%d"))

weekid=1
i=0
j=i+6
while i < len(date_list) and j < len(date_list) :
    weekid=weekid+1
    df_weekly1=dfmerged_dose1[(dfmerged_dose1['Date']>=date_list[i])&(dfmerged_dose1['Date']<=date_list[j])]
    updated_d1w=update_week1(df_weekly1,weekid)
    df_weekly2=dfmerged_dose2[(dfmerged_dose2['Date']>=date_list[i])&(dfmerged_dose2['Date']<=date_list[j])]
    updated_d2w=update_week2(df_weekly2,weekid)
    updated_d1w['dose2']=updated_d2w['dose2']
    df_weekly_district=pd.concat([df_weekly_district,updated_d1w],ignore_index=True)
    i=j+1
    j=i+6

df_weekly_district.sort_values(by=['District_Key','Week_id'], inplace = True)
df_weekly_district.to_csv('Outputs/district-vaccinated-count-weekly.csv',index=False)
#print('district-vaccinated-weekly.csv created')



#======================================================================================================
#STATEWISE


sdfmerged_dose1=pd.DataFrame({'dose1':dfr.groupby(['State_Code','Date'])['dose1'].sum()}).reset_index()
sdfmerged_dose2=pd.DataFrame({'dose2':dfr.groupby(['State_Code','Date'])['dose2'].sum()}).reset_index()


#------------------------------------------------------------------------------------------------------------
#overall statewise vaccination


state_overall_d1=pd.DataFrame({'dose1':sdfmerged_dose1.groupby(['State_Code'])['dose1'].max()}).reset_index()
state_overall_d2=pd.DataFrame({'dose2':sdfmerged_dose2.groupby(['State_Code'])['dose2'].max()}).reset_index()

f_state_overall=state_overall_d1.copy()
f_state_overall['Overall_id']=1
f_state_overall['dose2']=state_overall_d2['dose2']
f_state_overall=f_state_overall[['State_Code','Overall_id','dose1','dose2']]
f_state_overall.to_csv('Outputs/state-vaccinated-count-overall.csv',index=False)
#print('state-vaccinated-overall.csv created')
#-----------------------------------------------------------------------------------------------------------------
#correcting cumulative data for dose 1 to non cumulative
sdfmerged_dose1.loc[sdfmerged_dose1['State_Code'] == sdfmerged_dose1['State_Code'].shift(), 'newnumber'] = (sdfmerged_dose1.dose1 - sdfmerged_dose1.dose1.shift())
sdfmerged_dose1.loc[sdfmerged_dose1.newnumber.isna(),'newnumber'] = sdfmerged_dose1.dose1
sdfmerged_dose1['dose1'] = sdfmerged_dose1['newnumber']
sdfmerged_dose1.drop("newnumber", axis =1, inplace = True)
sdfmerged_dose1.loc[dfmerged_dose1['dose1']<0,'dose1']=0




#correcting cumulative data for dose 2 to non cumulative
sdfmerged_dose2.loc[sdfmerged_dose2['State_Code'] == sdfmerged_dose2['State_Code'].shift(), 'newnumber'] = (sdfmerged_dose2.dose2 - sdfmerged_dose2.dose2.shift())
sdfmerged_dose2.loc[sdfmerged_dose2.newnumber.isna(),'newnumber'] = sdfmerged_dose2.dose2
sdfmerged_dose2['dose2'] = sdfmerged_dose2['newnumber']
sdfmerged_dose2.drop("newnumber", axis =1, inplace = True)
sdfmerged_dose2.loc[dfmerged_dose2['dose2']<0,'dose2']=0

#----------------------------------------------------------------------------------------------------------
#statewise monthly vaccination

def supdate_month1(sdf_monthly1,monthid) :
    sdf_month1=sdf_monthly1.copy()
    sdf_month1['Month_id']=monthid
    sdf_month1 = pd.DataFrame({'dose1' : sdf_month1.groupby(['Month_id','State_Code']).sum()['dose1']}).reset_index()
    sdf_month1 = sdf_month1[['State_Code', 'Month_id', 'dose1']]
    return sdf_month1

def supdate_month2(sdf_monthly2,monthid) :
    sdf_month2=sdf_monthly2.copy()
    sdf_month2['Month_id']=monthid
    sdf_month2 = pd.DataFrame({'dose2' : sdf_month2.groupby(['Month_id','State_Code']).sum()['dose2']}).reset_index()
    sdf_month2 = sdf_month2[['State_Code', 'Month_id', 'dose2']]
    return sdf_month2

smonth_dose1=sdfmerged_dose1.copy()
smonth_dose2=sdfmerged_dose2.copy()

start_date= date(2021,1,15)
end_date=date(2021,8,14)
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
    sdf_monthly1=smonth_dose1[(smonth_dose1['Date']>=q[i]) & (smonth_dose1['Date']<=q[i+1])]
    supdated_monthly_df1=supdate_month1(sdf_monthly1,monthid)
    if firstcal == 1 :
        smonthly_final1=supdated_monthly_df1
        firstcal=0
    else :
       smonthly_final1=pd.concat([smonthly_final1,supdated_monthly_df1],ignore_index=True) 
    i=i+2

smonthly_final1.sort_values(by=['State_Code','Month_id'],inplace=True)

i=0
firstcal=1
monthid=0
while i < len(q)-1 :
    monthid=monthid+1
    sdf_monthly2=smonth_dose2[(smonth_dose2['Date']>=q[i]) & (smonth_dose2['Date']<=q[i+1])]
    supdated_monthly_df2=supdate_month2(sdf_monthly2,monthid)
    if firstcal == 1 :
        smonthly_final2=supdated_monthly_df2
        firstcal=0
    else :
       smonthly_final2=pd.concat([smonthly_final2,supdated_monthly_df2],ignore_index=True) 
    i=i+2

smonthly_final2.sort_values(by=['State_Code','Month_id'],inplace=True)
smonthly_final1['dose2']=smonthly_final2['dose2']
smonthly_final1.to_csv('Outputs/state-vaccinated-count-monthly.csv',index=False)
#print('state-vaccinated-monthly.csv created')

#--------------------------------------------------------------------------------------------------------------
#State Wise weekly vaccination


def update_week1s(sdf_weekly1,weekid) :
    sdf_weekly=sdf_weekly1.copy()
    l1=list([weekid]*sdf_weekly1.shape[0])
    sdf_weekly['Week_id']= l1
    sdf_weekly = pd.DataFrame({'dose1' : sdf_weekly.groupby(['Week_id','State_Code']).sum()['dose1']}).reset_index()
    sdf_weekly = sdf_weekly[['State_Code', 'Week_id', 'dose1']]
    return sdf_weekly


def update_week2s(sdf_weekly2,weekid) :
    sdf_weekly=sdf_weekly2.copy()
    l1=list([weekid]*sdf_weekly2.shape[0])
    sdf_weekly['Week_id']= l1
    sdf_weekly = pd.DataFrame({'dose2' : sdf_weekly.groupby(['Week_id','State_Code']).sum()['dose2']}).reset_index()
    sdf_weekly = sdf_weekly[['State_Code', 'Week_id', 'dose2']]
    return sdf_weekly

# For first week1 dose 1 and dose 2. First week is considered from 16/01/2020 to 23/01/2020

df_week1_state_d1=sdfmerged_dose1[(sdfmerged_dose1['Date']>='2021/01/16')&(sdfmerged_dose1['Date']<='2021/01/23')]
sdf_week1_d1=df_week1_state_d1.copy()
sdf_week1_d1['Week_id']=1
sdf_week1_d1 = pd.DataFrame({'dose1' : sdf_week1_d1.groupby(['Week_id','State_Code']).sum()['dose1']}).reset_index()
sdf_week1_d1 = sdf_week1_d1[['State_Code', 'Week_id', 'dose1']]

df_week1_state_d2=sdfmerged_dose2[(sdfmerged_dose2['Date']>='2021/01/16')&(sdfmerged_dose2['Date']<='2021/01/23')]
sdf_week1_d2=df_week1_state_d2.copy()
sdf_week1_d2['Week_id']=1
sdf_week1_d2 = pd.DataFrame({'dose2' : sdf_week1_d2.groupby(['Week_id','State_Code']).sum()['dose2']}).reset_index()
sdf_week1_d2 = sdf_week1_d2[['State_Code', 'Week_id', 'dose2']]
sdf_week1_d1['dose2']=sdf_week1_d2['dose2']
df_weekly_state=sdf_week1_d1


start_date= date(2021,1,24)
end_date= date(2021,8,14)

date_list=[]
for dt in daterange(start_date, end_date):
    date_list.append(dt.strftime("%Y-%m-%d"))

weekid=1
i=0
j=i+6
while i < len(date_list) and j < len(date_list) :
    weekid=weekid+1
    sdf_weekly1=sdfmerged_dose1[(sdfmerged_dose1['Date']>=date_list[i])&(sdfmerged_dose1['Date']<=date_list[j])]
    supdated_d1w=update_week1s(sdf_weekly1,weekid)
    sdf_weekly2=sdfmerged_dose2[(sdfmerged_dose2['Date']>=date_list[i])&(sdfmerged_dose2['Date']<=date_list[j])]
    supdated_d2w=update_week2s(sdf_weekly2,weekid)
    supdated_d1w['dose2']=supdated_d2w['dose2']
    df_weekly_state=pd.concat([df_weekly_state,supdated_d1w],ignore_index=True)
    i=j+1
    j=i+6

df_weekly_state.sort_values(by=['State_Code','Week_id'], inplace = True)
df_weekly_state.to_csv('Outputs/state-vaccinated-count-weekly.csv',index=False)
#print('state-vaccinated-weekly.csv created')































