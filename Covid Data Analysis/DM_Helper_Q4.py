#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 12:55:58 2021

@author: sharanya
"""
import pandas as pd
df=pd.read_csv('Outputs/cases-week.csv')
df_state_code=pd.read_csv('datasets/district_wise.csv')
df_state_code=df_state_code[['State','District_Key']]
state_code_dict=dict(zip(list(df_state_code['District_Key'].values),list(df_state_code['State'].values)))

df1=df.copy()
#replacing districts with their states
for key in state_code_dict:
    df['District'].replace(key,state_code_dict[key],inplace=True)
    
state_df_weekly = pd.DataFrame({'Cases' : df.groupby(['District','Week_id']).sum()['Cases']}).reset_index()
state_df_weekly.rename(columns = {'District':'State'}, inplace = True)


df_monthly=pd.read_csv('Outputs/cases-month.csv')
for key in state_code_dict:
    df_monthly['District'].replace(key,state_code_dict[key],inplace=True)
    
state_df_monthly = pd.DataFrame({'Cases' : df_monthly.groupby(['District','Month_id']).sum()['Cases']}).reset_index()
state_df_monthly.rename(columns = {'District':'State'}, inplace = True)
state_df_weekly.to_csv('datasets/cases-week-statewise.csv', index = False) 
state_df_monthly.to_csv('datasets/cases-month-statewise.csv', index = False) 
