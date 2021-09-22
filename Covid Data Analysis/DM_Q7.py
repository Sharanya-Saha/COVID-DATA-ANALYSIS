#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 23:47:18 2021

@author: sharanya
"""
import pandas as pd
from datetime import timedelta, date


df=pd.read_csv('datasets/cowin_vaccine_data_districtwise.csv',low_memory=False)
df=df.drop(0)
df.reset_index()

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
        req['State'] = df.loc[i, 'State']
        req['District_Key'] = df.loc[i, 'District_Key']
        req['District']=df.loc[i,'District']
        req['Date'] = dates[j]
        req['covaxin'] = df.loc[i,dates[j]+'.8']
        req['covishield'] = df.loc[i,dates[j]+'.9']
        required.append(req)

dfr=pd.DataFrame(required)
dfr['Date'] = pd.to_datetime(dfr['Date'], format='%d/%m/%Y')
dfr=dfr.sort_values(['District_Key','Date'])

dfr['covaxin'].fillna(0,inplace=True)
dfr['covishield'].fillna(0,inplace=True)
dfr['covaxin']=dfr['covaxin'].astype(int)
dfr['covishield']=dfr['covishield'].astype(int)


#=========================================================================================================================
#statewise vaccine type ratio


dfmerged_state=pd.DataFrame({'covaxin':dfr.groupby(['State','Date'])['covaxin'].sum(),'covishield':dfr.groupby(['State','Date'])['covishield'].sum()}).reset_index()
sdfr=pd.DataFrame({'covaxin':dfmerged_state.groupby(['State'])['covaxin'].max(),'covishield':dfmerged_state.groupby(['State'])['covishield'].max()}).reset_index()
sdfr['vaccineratio']=sdfr['covishield']/sdfr['covaxin']
sdfr=sdfr[['State','vaccineratio']]
sdfr.sort_values(by=['vaccineratio'],inplace=True)
sdfr.to_csv('Outputs/state-vaccine-type-ratio.csv',index=False)


#====================================================================================================================================
#Country vaccine type ratio


ofdr=pd.DataFrame({'covaxin':dfr.groupby(['Date'])['covaxin'].sum(),'covishield':dfr.groupby(['Date'])['covishield'].sum()}).reset_index()
ofdr['Country']='India'
overall_india=pd.DataFrame({'covaxin':ofdr.groupby(['Country'])['covaxin'].max(),'covishield':ofdr.groupby(['Country'])['covishield'].max()}).reset_index()
overall_india['vaccineratio']=overall_india['covishield']/overall_india['covaxin']
overall_india=overall_india[['Country','vaccineratio']]
overall_india.sort_values(by=['vaccineratio'],inplace=True)
overall_india.to_csv('Outputs/overall-vaccine-type-ratio.csv',index=False)

#====================================================================================================================
# District wise vaccine type ratio


dfmerged_district=pd.DataFrame({'covaxin':dfr.groupby(['District_Key','Date'])['covaxin'].sum(),'covishield':dfr.groupby(['District_Key','Date'])['covishield'].sum()}).reset_index()
ddfr=pd.DataFrame({'covaxin':dfmerged_district.groupby(['District_Key'])['covaxin'].max(),'covishield':dfmerged_district.groupby(['District_Key'])['covishield'].max()}).reset_index()
#ddfr.loc[ddfr.covaxin==0,'covaxin']=0.1
ddfr['vaccineratio']=ddfr['covishield']/ddfr['covaxin']
ddfr=ddfr[['District_Key','vaccineratio']]
ddfr.sort_values(by=['vaccineratio'],inplace=True)
ddfr.to_csv('Outputs/district-vaccine-type-ratio.csv',index=False)


#==============================================================================================================
