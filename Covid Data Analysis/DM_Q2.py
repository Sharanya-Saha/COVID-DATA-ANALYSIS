#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 18:41:10 2021

@author: sharanya
"""

import json
import csv


read=open('Outputs/neighbor-districts-modified.json')
data=json.load(read)

edges=[]

for dist in data :
    for neighbour in data[dist] :
        if [neighbour,dist] not in edges :
            edges.append([dist,neighbour])
            
with open('Outputs/edge-graph.csv', 'w+', newline ='') as f:     
    w = csv.writer(f) 
    w.writerows(edges) 
    
#print('edge-graph.csv has been created')
