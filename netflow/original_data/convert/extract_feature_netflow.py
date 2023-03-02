# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 16:02:48 2018

@author: jia-ming
"""

###########################################
##########extract netflow feature##########
###########################################

from os import walk
import pandas as pd

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

now_my_path = "../convert"

all_file_name = []

for path, folder, files in walk(now_my_path):
    for file_name in files:
        all_file_name.append(file_name)


all_file_name.remove("extract_feature_netflow.py")

list_total_bytes = []
list_total_link_count = []
list_datetime = []

total_bytes = 0

#target ip#
extract_ip = '10.10.62.79'

import humanfriendly
import datetime


for name in all_file_name:
    print(name)   
    
    #read csv#    
    df = pd.read_csv(name, encoding = "utf-8")
    
    df["時間"] = pd.to_datetime(df["時間"])
    tmp = df["時間"][0].date()
    tmp_date = tmp.strftime('%Y-%m-%d')
    
    #Extract target IP#
    df = df[df.來源IP.isin([extract_ip]) | df.目的IP.isin([extract_ip])]

    #Counting flow and link per minute #
    df = df.set_index("時間")
    big_group = df.groupby([df.index.year,df.index.month,df.index.day,df.index.hour,df.index.minute])
    for date,small_group in big_group:
        ####time#####
        tmp = datetime.datetime(date[0],date[1],date[2],date[3],date[4])
        list_datetime.append(tmp)

        ###one minute bytes###
        bytes_series = small_group["Bytes"]
        for i in bytes_series:
            number_bytes =  humanfriendly.parse_size(i)
            total_bytes =  total_bytes + number_bytes
        list_total_bytes.append(total_bytes)
        total_bytes = 0
        number_bytes = 0
        
        ###total link###
        tmp_tuple = small_group.shape
        list_total_link_count.append(tmp_tuple[0])
    
    #save dataframe#
    total_bytes = pd.DataFrame(list_total_bytes,columns=["total_bytes"])
    total_link_count = pd.DataFrame(list_total_link_count,columns = ["total_link_count"])
    total_date = pd.DataFrame(list_datetime,columns = ["date"])
    
    frames = [total_date,total_bytes, total_link_count]
    df_all = pd.concat(frames,axis=1)
    
    #####generate time range#####
    table_date = [] 
    start = tmp_date + ' 00:00:00'
    end = tmp_date + ' 23:59:00'
    
    date_start=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    date_end=datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
     
    while date_start<=date_end:
        table_date.append(date_start)
        date_start+=datetime.timedelta(minutes=1)
        
    table_date = pd.DataFrame(table_date,columns = ["date"])
    
    #####merge and sort#######
    result = pd.merge(df_all, table_date, how='outer', on=['date'])
    result = result.fillna(0)
    result.sort_values('date', inplace=True)
    print(result)
    
    save_name = extract_ip+"_"+ tmp_date+"_"+ "extract"
    result.to_csv(save_name,index = False, encoding ='utf-8')

    #clear#
    total_bytes = 0
    list_total_bytes.clear()
    list_total_link_count.clear()
    list_datetime.clear()



