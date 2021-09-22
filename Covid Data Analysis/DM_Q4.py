#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 10:29:58 2021

@author: sharanya
"""

import pandas as pd


df = pd.read_csv('Outputs/cases-week.csv')
district_id = list(set(df['District'].values))
district_id=sorted(district_id)
df_month=pd.read_csv('Outputs/cases-month.csv')
# wave1 range approximately (20-65)
# wave1 range approximately (95-120)

df_wave1=df[(df['Week_id']>=20) & (df['Week_id']<=65)]
df_wave2=df[(df['Week_id']>=95) & (df['Week_id']<=120)]
dfm_wave1=df_month[(df_month['Month_id']>=2)&(df_month['Month_id']<=8)]
dfm_wave2=df_month[(df_month['Month_id']>=11)&(df_month['Month_id']<=17)]

peaks={}
for district in district_id :
    rows=[]
    rows.append(district)
    df_dist1=df_wave1[df_wave1['District']==district].reset_index()
    try :
        maxloc = df_dist1['Cases'].idxmax()
        weekid=df_dist1['Week_id'].iloc[maxloc]
        rows.append(weekid)
    except :
        rows.append(0)
    df_dist2=df_wave2[df_wave2['District']==district].reset_index()
    try:
        maxloc = df_dist2['Cases'].idxmax()
        weekid=df_dist2['Week_id'].iloc[maxloc]
        rows.append(weekid)
    except:
        rows.append(0)
    dfm_dist1=dfm_wave1[dfm_wave1['District']==district].reset_index()
    try :
        maxloc = dfm_dist1['Cases'].idxmax()
        monthid=dfm_dist1['Month_id'].iloc[maxloc]
        rows.append(monthid)
    except :
        rows.append(0)
    dfm_dist2=dfm_wave2[dfm_wave2['District']==district].reset_index()
    try :
        maxloc = dfm_dist2['Cases'].idxmax()
        monthid=dfm_dist2['Month_id'].iloc[maxloc]
        rows.append(monthid)
    except :
        rows.append(0)
    peaks[district]=rows
    
df1=pd.DataFrame.from_dict(peaks, orient='index',columns=['District_id', 'Wave1-weekid' ,'Wave2-weekid','Wave1-monthid','Wave2-monthid'])

df1.to_csv('Outputs/districts-peaks.csv', index = False)
#print('Outputs/districts-peaks.csv is created')

#-----------------------------------------------------------------------------------------------------------------------------------------------------------

#For statewise peaks


df_state_week=pd.read_csv('datasets/cases-week-statewise.csv')
df_state_month=pd.read_csv('datasets/cases-month-statewise.csv')


dfw_wave1_state=df_state_week[(df_state_week['Week_id']>=15) & (df_state_week['Week_id']<=65)]
dfw_wave2_state=df_state_week[(df_state_week['Week_id']>=95) & (df_state_week['Week_id']<=120)]
dfm_wave1_state=df_state_month[(df_state_month['Month_id']>=2) & (df_state_month['Month_id']<=8)]
dfm_wave2_state=df_state_month[(df_state_month['Month_id']>=11) & (df_state_month['Month_id']<=17)]

state_list=list(set(df_state_week['State'].values))

peaks_state={}
for state in state_list :
    rows=[]
    rows.append(state)
    df_state1=dfw_wave1_state[dfw_wave1_state['State']==state].reset_index()
    try :
        maxloc = df_state1['Cases'].idxmax()
        weekid=df_state1['Week_id'].iloc[maxloc]
        rows.append(weekid)
    except :
        rows.append(0)
    df_state2=dfw_wave2_state[dfw_wave2_state['State']==state].reset_index()
    try:
        maxloc = df_state2['Cases'].idxmax()
        weekid=df_state2['Week_id'].iloc[maxloc]
        rows.append(weekid)
    except:
        rows.append(0)
    dfm_state1=dfm_wave1_state[dfm_wave1_state['State']==state].reset_index()
    try :
        maxloc = dfm_state1['Cases'].idxmax()
        monthid=dfm_state1['Month_id'].iloc[maxloc]
        rows.append(monthid)
    except :
        rows.append(0)
    dfm_state2=dfm_wave2_state[dfm_wave2_state['State']==state].reset_index()
    try :
        maxloc = dfm_state2['Cases'].idxmax()
        monthid=dfm_state2['Month_id'].iloc[maxloc]
        rows.append(monthid)
    except :
        rows.append(0)
    peaks_state[state]=rows
    
df2=pd.DataFrame.from_dict(peaks_state, orient='index',columns=['State', 'Wave1-weekid' ,'Wave2-weekid','Wave1-monthid','Wave2-monthid'])
df2['State'].replace('DL_Delhi','Delhi',inplace=True)
df2=df2.sort_values(['State'],ascending=[True])
df2.to_csv('Outputs/state-peaks.csv', index = False)
#print('state-peaks.csv is created')
#-------------------------------------------------------------------------------------------------------------------------------------------------

#for overall peaks


overall_df = pd.DataFrame({'Cases' : df_state_week.groupby('Week_id').sum()['Cases']}).reset_index()
overall_dfw_wave1=overall_df[(overall_df['Week_id']>=20) & (overall_df['Week_id']<=65)].reset_index()
overall_dfw_wave2=overall_df[(overall_df['Week_id']>=95) & (overall_df['Week_id']<=120)].reset_index()
maxloc_wave1_week=overall_dfw_wave1['Cases'].idxmax()
peak_weekid_wave1_overall=overall_dfw_wave1['Week_id'].iloc[maxloc_wave1_week]
maxloc_wave2_week=overall_dfw_wave2['Cases'].idxmax()
peak_weekid_wave2_overall=overall_dfw_wave2['Week_id'].iloc[maxloc_wave2_week]

overall_dfm=pd.DataFrame({'Cases' : df_state_month.groupby('Month_id').sum()['Cases']}).reset_index()
overall_dfm_wave1=overall_dfm[(overall_dfm['Month_id']>=2) & (overall_dfm['Month_id']<=8)].reset_index()
overall_dfm_wave2=overall_dfm[(overall_dfm['Month_id']>=11) & (overall_dfm['Month_id']<=17)].reset_index()
maxloc_wave1_month=overall_dfm_wave1['Cases'].idxmax()
peak_monthid_wave1_overall=overall_dfm_wave1['Month_id'].iloc[maxloc_wave1_month]
maxloc_wave2_month=overall_dfm_wave2['Cases'].idxmax()
peak_monthid_wave2_overall=overall_dfm_wave2['Month_id'].iloc[maxloc_wave2_month]

df3=[]
df3.append(['1',peak_weekid_wave1_overall,peak_weekid_wave2_overall,peak_monthid_wave1_overall,peak_monthid_wave2_overall])
df3=pd.DataFrame(df3,columns = ['Overall','Wave1-weekid','Wave2-weekid','Wave1-monthid','Wave2-monthid'] )
df3.to_csv('Outputs/overall-peaks.csv', index = False)
#print('overall-peaks.csv is created')


