#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 18:54:46 2021

@author: sharanya
"""

import pandas as pd
from datetime import timedelta, date

df = pd.read_excel('datasets/DDW_PCA0000_2011_Indiastatedist.xlsx',engine='openpyxl')
columns=['State','District','Level','Name','TRU','No_HH','TOT_P','TOT_M','TOT_F']
df=df[columns]

df = df.loc[df['TRU']=='Total']

df_district=df.loc[df['Level']=='DISTRICT']
df_state=df.loc[df['Level']=='STATE']
#state_list_census=list(df_state['Name'].str.lower())


df1=pd.read_csv('datasets/cowin_vaccine_data_districtwise.csv',low_memory=False)
df1=df1.drop(0)
df1.reset_index()

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
    for i in range(1,len(df1)+1) :
            req = {}
            req['State']=df1.loc[i,'State']
            req['State_Code'] = df1.loc[i, 'State_Code']
            req['District_Key'] = df1.loc[i, 'District_Key']
            req['District']=df1.loc[i,'District']
            req['Date'] = dates[j]
            req['dose1'] = df1.loc[i,dates[j]+'.3']
            req['dose2'] = df1.loc[i,dates[j]+'.4']
            required.append(req)
           

    
dfr=pd.DataFrame(required)
dfr['Date'] = pd.to_datetime(dfr['Date'], format='%d/%m/%Y')
dfr=dfr.sort_values(['District_Key','Date'])

dfr['dose1'].fillna(0,inplace=True)
dfr['dose2'].fillna(0,inplace=True)
dfr['dose1']=dfr['dose1'].astype(int)
dfr['dose2']=dfr['dose2'].astype(int)

df_state['Name']=df_state['Name'].str.lower()
dfr['State']=dfr['State'].str.lower()

#removing Telangana and Ladakh from cowin data
dfr = dfr.loc[dfr['State']!='ladakh']
dfr = dfr.loc[dfr['State']!='telangana']

#renaming nct of delhi to delhi in census data
#renaming jammu & kashmir to jammu and kashmir in census data
#renaming andaman & nicobar islands to andaman and nicobar islands
df_state.loc[df_state.Name == "nct of delhi" , "Name"] = "delhi"
df_state.loc[df_state.Name == "andaman & nicobar islands" , "Name"] = "andaman and nicobar islands"
df_state.loc[df_state.Name == "jammu & kashmir" , "Name"] = "jammu and kashmir"
df_state.loc[df_state.Name == "dadra & nagar haveli" , "Name"] = "dadra and nagar haveli and daman and diu"
df_state.loc[df_state.Name == "daman & diu" , "Name"] = "dadra and nagar haveli and daman and diu"


corrected_state_census=pd.DataFrame({'TOT_P':df_state.groupby(['Name'])['TOT_P'].sum(),'TOT_M':df_state.groupby(['Name'])['TOT_M'].sum(),'TOT_F':df_state.groupby(['Name'])['TOT_F'].sum()}).reset_index()

dfmerged_state=pd.DataFrame({'dose1':dfr.groupby(['State','Date'])['dose1'].sum(),'dose2':dfr.groupby(['State','Date'])['dose2'].sum()}).reset_index()
sdfr=pd.DataFrame({'dose1':dfmerged_state.groupby(['State'])['dose1'].max(),'dose2':dfmerged_state.groupby(['State'])['dose2'].max()}).reset_index()
corrected_state_census['dose1']=sdfr['dose1']
sdfr['vaccinateddose1ratio']=sdfr['dose1']/corrected_state_census['TOT_P']
sdfr['vaccinateddose2ratio']=sdfr['dose2']/corrected_state_census['TOT_P']
sdfr=sdfr[['State','vaccinateddose1ratio','vaccinateddose2ratio']]
sdfr.sort_values(by=['vaccinateddose1ratio'],inplace=True)
sdfr.to_csv('Outputs/state-vaccinated-dose-ratio.csv',index=False)
corrected_state_census=corrected_state_census[['Name','TOT_P','dose1']]

#Only printed to use in Question 9
corrected_state_census.to_csv('datasets/statewise_vaccinated.csv',index=False)

ofdr=pd.DataFrame({'dose1':dfr.groupby(['Date'])['dose1'].sum(),'dose2':dfr.groupby(['Date'])['dose2'].sum()}).reset_index()
ofdr['Country']='India'
overall_india=pd.DataFrame({'dose1':ofdr.groupby(['Country'])['dose1'].max(),'dose2':ofdr.groupby(['Country'])['dose2'].max()}).reset_index()
df_india=df.loc[df['Level']=='India']
overall_india['vaccinateddose1ratio']=overall_india['dose1']/df_india['TOT_P']
overall_india['vaccinateddose2ratio']=overall_india['dose2']/df_india['TOT_P']
overall_india=overall_india[['Country','vaccinateddose1ratio','vaccinateddose2ratio']]
overall_india.sort_values(by=['vaccinateddose1ratio'], inplace = True)
overall_india.to_csv('Outputs/overall-vaccinated-dose-ratio.csv',index=False)



df_district['Name']=df_district['Name'].str.strip()
df_district.loc[df_district.Name=='Y.S.R.','Name']='Y.S.R. Kadapa'
df_district.loc[df_district.Name=='Ahmadabad','Name']='Ahmedabad'
df_district.loc[df_district.Name=='Bara Banki','Name']='Barabanki'
df_district.loc[df_district.Name=='Bandipore','Name']='Bandipora'
df_district.loc[df_district.Name=='Bid','Name']='Beed'
df_district.loc[df_district.Name=='Badgam','Name']='Budgam'
df_district.loc[df_district.Name=='Ahmadnagar','Name']='Ahmednagar'
df_district.loc[df_district.Name=='Bellary','Name']='Ballari'
df_district.loc[df_district.Name=='Baleshwar','Name']='Balasore'
df_district.loc[df_district.Name=='Banas Kantha','Name']='Banaskantha'
df_district.loc[df_district.Name=='Darjiling','Name']='Darjeeling'
df_district.loc[df_district.Name=='Chikmagalur','Name']='Chikkamagaluru'
df_district.loc[df_district.Name=='Chittaurgarh','Name']='Chittorgarh'
df_district.loc[df_district.Name=='Dadra & Nagar Haveli','Name']='Dadra and Nagar Haveli'
df_district.loc[df_district.Name=='Dhaulpur','Name']='Dholpur'
df_district.loc[df_district.Name=='Maldah','Name']='Malda'
df_district.loc[df_district.Name=='Mysore','Name']='Mysuru'
df_district.loc[df_district.Name=='Narsimhapur','Name']='Narsinghpur'
df_district.loc[df_district.Name=='Dibang Valley','Name']='Upper Dibang Valley'
df_district.loc[df_district.Name=='Mahesana','Name']='Mehsana'
df_district.loc[df_district.Name=='Muktsar','Name']='Sri Muktsar Sahib'
df_district.loc[df_district.Name=='Mahrajganj','Name']='Maharajganj'
df_district.loc[df_district.Name=='North','Name']='North Delhi'
df_district.loc[df_district.Name=='Central','Name']='Central Delhi'
df_district.loc[df_district.Name=='North East','Name']='North East Delhi'
df_district.loc[df_district.Name=='North West','Name']='North West Delhi'
df_district.loc[df_district.Name=='West','Name']='West Delhi'
df_district.loc[df_district.Name=='East','Name']='East Delhi'
df_district.loc[df_district.Name=='South','Name']='South Delhi'
df_district.loc[df_district.Name=='South West','Name']='South West Delhi'
df_district.loc[df_district.Name=='North  District','Name']='North Sikkim'
df_district.loc[df_district.Name=='West District','Name']='West Sikkim'
df_district.loc[df_district.Name=='East District','Name']='East Sikkim'
df_district.loc[df_district.Name=='South District','Name']='South Sikkim'


dfr = dfr.loc[dfr['District_Key']!='CT_Balrampur']

rename={'Baudh':'Boudh','Anugul':'Angul','Bagalkot':'Bagalkote','Baramula':'Baramulla','Buldana':'Buldhana','Debagarh':	'Deogarh','Kachchh':'Kutch','Hardwar':'Haridwar','Hugli':'Hooghly','Haora':'Howrah','Chamarajanagar':'Chamarajanagara','Panch Mahals':'Panchmahal','Puruliya':'Purulia','Purba Champaran':'East Champaran','Pashchim Champaran':'West Champaran','Pashchimi Singhbhum':'East Singhbhum','Purbi Singhbhum':'West Singhbhum','Allahabad':'Prayagraj','The Dangs':'Dang','The Nilgiris':'Nilgiris','Koch Bihar':'Cooch Behar','Jhunjhunun':'Jhunjhunu','Jagatsinghapur':'Jagatsinghpur','Jajapur':'Jajpur','Jalor':'Jalore','Janjgir - Champa':'Janjgir Champa'}

for district in rename :
    df_district.loc[df_district.Name==district,'Name']=rename[district]


rename1={'Bangalore':'Bengaluru Urban','Bangalore Rural':'Bengaluru Rural','Dohad':'Dahod','Kaimur (Bhabua)':'Kaimur','Kanniyakumari':'Kanyakumari','Kodarma':'Koderma','Khandwa (East Nimar)':'Khandwa','Khargone (West Nimar)':'Khargone','Kheri':'Lakhimpur Kheri','Kanshiram Nagar':'Kasganj','Lahul & Spiti':'Lahaul and Spiti','Gondiya':'Gondia','North Twenty Four Parganas':'North 24 Parganas','South Twenty Four Parganas':'South 24 Parganas','North  & Middle Andaman':'North and Middle Andaman','Belgaum':'Belagavi','Firozpur':'Ferozepur','Garhwal':'Pauri Garhwal','Faizabad':'Ayodhya','Gulbarga':'Kalaburagi','Gurgaon':'Gurugram','Jyotiba Phule Nagar':'Amroha','Mahamaya Nagar':'Hathras','Sant Ravidas Nagar (Bhadohi)':'Bhadohi','Sri Potti Sriramulu Nellore':'S.P.S. Nellore','Sahibzada Ajit Singh Nagar':'S.A.S. Nagar','Tumkur':'Tumakuru','Sabar Kantha':'Sabarkantha','Shimoga':'Shivamogga','Shupiyan':'Shopiyan'}

for district in rename1 :
    df_district.loc[df_district.Name==district,'Name']=rename1[district]



discard=['Mewat','Nizamabad','Mahbubnagar','Mumbai Suburban','Medak','Nalgonda','Karimnagar','Khammam','Warangal','Leh(Ladakh)','Kargil','Rangareddy','Hyderabad','Adilabad']
for district in discard :
    df_district=df_district.loc[df_district['Name'] != district]




# Districts Purba Bardhaman and Paschim Bardhamn are merged to match with census data
dfr.loc[dfr.District=='Paschim Bardhaman','District']='Barddhaman'
dfr.loc[dfr.District=='Purba Bardhaman','District']='Barddhaman'
dfr.loc[dfr.District=='Barddhaman','District_Key']='WB_Barddhaman'
# Districts East Jaintia Hills and West Jaintia Hills are merged to match with census data
dfr.loc[dfr.District=='East Jaintia Hills','District']='Jaintia Hills'
dfr.loc[dfr.District=='West Jaintia Hills','District']='Jaintia Hills'
dfr.loc[dfr.District=='Jaintia Hills','District_Key']='ML_Jaintia Hills'



drop=['Fazilka', 'Charaideo', 'Shahdara', 'Kalimpong', 'Kangpokpi', 'Pakke Kessang', 'Tengnoupal', 'Biswanath', 'Mungeli', 'Sipahijala', 'Sambhal', 'South East Delhi', 'Surajpur', 'Jhargram', 'Bametara', 'South West Khasi Hills', 'Kakching', 'Kamjong', 'Noney', 'Kondagaon', 'Baloda Bazar', 'Aravalli', 'Palghar', 'Shi Yomi', 'Pherzawl', 'Pathankot', 'Namsai', 'Nuh', 'Gariaband', 'Gir Somnath', 'Kamle', 'West Karbi Anglong', 'Hapur', 'Gomati', 'South Salmara Mankachar', 'Khowai', 'Majuli', 'Unokoti', 'Sukma', 'Tenkasi', 'South West Garo Hills', 'Chhota Udaipur', 'Tirupathur', 'Kra Daadi', 'Alipurduar', 'Morbi', 'Agar Malwa', 'Gaurela Pendra Marwahi', 'Lepa Rada', 'Lower Siang', 'Chengalpattu', 'Longding', 'Devbhumi Dwarka', 'Botad', 'Balod', 'Charkhi Dadri', 'Siang', 'Kallakurichi', 'Shamli', 'Amethi', 'Jiribam', 'Mahisagar', 'North Garo Hills', 'Ranipet', 'Hojai']
for district in drop :
    dfr=dfr.loc[dfr['District']!=district]

district_code_dict=dict(zip(list(dfr['District'].values),list(dfr['District_Key'].values)))

# For districts with different names and same district key
dfmerged=pd.DataFrame({'dose1':dfr.groupby(['District_Key','Date'])['dose1'].sum(),'dose2':dfr.groupby(['District_Key','Date'])['dose2'].sum()}).reset_index()


df_district.loc[(df_district.Name=='Aurangabad') & (df_district.State==10),'Name']='BR_Aurangabad'
df_district.loc[(df_district.Name=='Aurangabad') & (df_district.State==27),'Name']='MH_Aurangabad'
df_district.loc[(df_district.Name=='Bilaspur') & (df_district.State==2),'Name']='HP_Bilaspur'
df_district.loc[(df_district.Name=='Bilaspur') & (df_district.State==22),'Name']='CT_Bilaspur'
df_district.loc[(df_district.Name=='Hamirpur') & (df_district.State==2),'Name']='HP_Hamirpur'
df_district.loc[(df_district.Name=='Hamirpur') & (df_district.State==9),'Name']='UP_Hamirpur'
df_district.loc[(df_district.Name=='Pratapgarh') & (df_district.State==8),'Name']='RJ_Pratapgarh'
df_district.loc[(df_district.Name=='Pratapgarh') & (df_district.State==9),'Name']='UP_Pratapgarh'
df_district.loc[(df_district.Name=='Bijapur') & (df_district.State==29),'Name']='KA_Vijayapura'
df_district.loc[(df_district.Name=='Bijapur') & (df_district.State==22),'Name']='CT_Bijapur'
df_district.loc[(df_district.Name=='Raigarh')&(df_district.State==27),'Name']='MH_Raigad'

for district in district_code_dict :
    df_district.loc[df_district.Name==district,'Name']=district_code_dict[district]


df_district.sort_values(by=['Name'],inplace=True)
df_district=df_district.reset_index()

overall_district=pd.DataFrame({'dose1':dfmerged.groupby(['District_Key'])['dose1'].max(),'dose2':dfmerged.groupby(['District_Key'])['dose2'].max()}).reset_index()
overall_district['vaccinateddose1ratio']=overall_district['dose1']/df_district['TOT_P']
overall_district['vaccinateddose2ratio']=overall_district['dose2']/df_district['TOT_P']
overall_district=overall_district[['District_Key','vaccinateddose1ratio','vaccinateddose2ratio']]
overall_district.sort_values(by=['vaccinateddose1ratio'], inplace = True)
overall_district.to_csv('Outputs/district-vaccinated-dose-ratio.csv',index=False)




