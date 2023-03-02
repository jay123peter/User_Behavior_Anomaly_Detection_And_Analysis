# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 16:02:48 2018

@author: jia-ming
"""

from os import walk
import pandas as pd
import datetime

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

now_my_path = "../dtw_convert"

all_file_name = []

for path, folder, files in walk(now_my_path):
    for file_name in files:
        all_file_name.append(file_name)

all_file_name.remove("dtw_file_server_extract.py")

list_total_read = []
list_datetime = []

total_read = 0

for name in all_file_name:
    print(name)  
    
    df = pd.read_csv(name, encoding = "utf-8")
    df = df[df['event']=='read']
    
    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time")
    
    #user_name = df['user'][0].split('\\')[1]
    user_name = df['user'][0]
    
    day_group = df.groupby([df.index.year,df.index.month,df.index.day]) #,df.index.hour ,df.index.minute
    for date,small_day_group in day_group:
        print(date)

        day_hour_group = small_day_group.groupby([small_day_group.index.year,small_day_group.index.month,small_day_group.index.day,small_day_group.index.hour,small_day_group.index.minute]) #,small_day_group.index.minute
        for day_hour,small_day_hour_group in day_hour_group:
            print(day_hour)
    
            ####time#####
            tmp = datetime.datetime(day_hour[0],day_hour[1],day_hour[2],day_hour[3],day_hour[4]) #,day_hour[4]
            list_datetime.append(tmp)
    
            ###one hour read###
            event_shape = small_day_hour_group.shape
            list_total_read.append(event_shape[0])
         
        total_date = pd.DataFrame(list_datetime,columns = ["date"])
        total_read = pd.DataFrame(list_total_read,columns=["total_read"])
        
        frames = [total_date,total_read]
        df_all = pd.concat(frames,axis=1)

        #####range#####
        table_date = [] 
       
        #convert tuple to datetime
        tmp = datetime.datetime(date[0],date[1],date[2])
        #datetime to string
        string_date = tmp.strftime("%Y-%m-%d")  #%H:%M:%S
        start = string_date + ' 00:00:00'
        end = string_date + ' 23:59:00'
        
        date_start=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
        date_end=datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
         
        while date_start<=date_end:
            table_date.append(date_start)
            date_start+=datetime.timedelta(minutes=1) #hours=1 #minutes=1
            
        table_date = pd.DataFrame(table_date,columns = ["date"])
 
        #####merge and sort#######
  
        result = pd.merge(df_all, table_date, how='outer', on=['date'])
        result = result.fillna(0)
        result.sort_values('date', inplace=True)
        #print(result)

        save_name = user_name+ "_" + string_date + "_" + "extract.csv"
        result.to_csv(save_name,index = False, encoding ='utf-8')
        
        total_read = 0
        list_total_read.clear()
        list_datetime.clear()




