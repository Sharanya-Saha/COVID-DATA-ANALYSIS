#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 13:51:39 2021

@author: sharanya
"""

import json
import pandas as pd



def replace_all(replace,key_val) :   
    temp_dict=new_data_neighbours.copy()
    for keys in temp_dict :
        
        keys_neighbour=new_data_neighbours[keys]
        
        if keys == key_val :
             new_data_neighbours[replace] = new_data_neighbours.pop(key_val)
             
        for i in range(len(keys_neighbour)) :
            if keys_neighbour[i] == key_val :
                keys_neighbour[i]=replace
                
def delete_all(key_val) :
    temp_dict=new_data_neighbours.copy()
    for keys in temp_dict :
        keys_neighbour=new_data_neighbours[keys]
        if keys == key_val :
            del new_data_neighbours[keys]
        temp_list=keys_neighbour.copy()
        
        for i in range(len(temp_list)) :
            if temp_list[i] == key_val :
                keys_neighbour.remove(key_val)
                             
                
read=open('datasets/neighbor-districts.json')
data_neighbours=json.load(read)

vaccine_csv=pd.read_csv('datasets/cowin_vaccine_data_districtwise.csv', low_memory=False)
print(vaccine_csv.columns)
column_names=['District_Key','District']
districts=vaccine_csv[column_names].copy()


district_codes=list(districts['District_Key'].values)
district_names=list(districts['District'].values)
dict_districts=dict(zip(district_names,district_codes))


# Creates a dictionary with the lower case district names as key value
lower_upper_districts={}
for i in range(len(district_names)) :
   try :
       key = district_names[i].lower()
       lower_upper_districts[key]=district_names[i]
   except:
       pass


#Spellings.csv has the corrected spellings. The spellings has been corrected manually in the csv .
spelling_replace=pd.read_csv('datasets/Spelling.csv')
spell_rename=dict(zip(list(spelling_replace['Neighbour_data']),list(spelling_replace['Vaccinated_data'])))

new_data_neighbours=data_neighbours.copy()      

#districts with same names
replace_all("HP_Bilaspur","bilaspur/Q1478939")
replace_all("CT_Bilaspur","bilaspur/Q100157")
replace_all("RJ_Pratapgarh","pratapgarh/Q1585433")
replace_all("UP_Pratapgarh","pratapgarh/Q1473962")
replace_all("HP_Hamirpur","hamirpur/Q2086180")
replace_all("UP_Hamirpur","hamirpur/Q2019757")
replace_all("CT_Bijapur", "bijapur/Q100164")
replace_all("KA_Vijayapura", "bijapur_district/Q1727570")
replace_all("BR_Aurangabad", "aurangabad/Q43086")
replace_all("MH_Aurangabad","aurangabad/Q592942")
replace_all("UP_Balrampur","balrampur/Q1948380")
replace_all("CT_Balrampur","balrampur/Q16056268")
    
for key in data_neighbours :
    key_split=key.split('/')
    key_split=key_split[0]
    if '_district' in key_split :
        key_split = key_split.replace('_district','')
        key_split.strip()
    key_split=key_split.replace('_',' ')
    
    if key_split in lower_upper_districts :
        key_name = lower_upper_districts[key_split]
        replace = dict_districts[key_name]
        #print(key_name,replace)
        replace_all(replace, key)
    
    elif key_split in spell_rename :
        key_name = spell_rename[key_split]
        key_name_1 = lower_upper_districts[key_name]
        replace = dict_districts[key_name_1]
        #print(key_name,replace)
        replace_all(replace, key)
    
        
#Merging Mumbai_suburban and Mumbai city  with MH_Mumbai
delete_all("mumbai_city/Q2341660")
replace_all("MH_Mumbai","mumbai_suburban/Q2085374")

#removing districts which are not found in covid19 vaccination data
delete_all("konkan_division/Q6268840")
delete_all('niwari/Q63563797')
delete_all('noklak/Q48731903')


with open('Outputs/neighbor-districts-modified.json','w') as json_file :
    json.dump(new_data_neighbours,json_file,indent=4,separators=(',',':'),sort_keys=True)
#print('neighbor-districts-modified.json is created')
