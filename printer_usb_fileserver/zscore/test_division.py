# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 17:08:51 2019

@author: jia-ming
"""

########################################
#######zscore test data division########
########################################


import pandas as pd

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

df = pd.read_csv("190704_filexferlog_UUID_UTF8_(Security B).txt" , encoding = "utf-8",delimiter = ",",engine ="c",error_bad_lines=False,warn_bad_lines=True) #,chunksize=read_size
#df = pd.read_csv("FileServer_UUID_Log_20190611_(Security B).txt" , encoding = "Big5",delimiter = ",",engine ="c",error_bad_lines=False,warn_bad_lines=True) #,chunksize=read_size  
#df = pd.read_csv("FileServer_UUID_Log_20190527_(Security B).txt" , encoding = "Big5",delimiter = "\t",engine ="c",error_bad_lines=False,warn_bad_lines=True) #,chunksize=read_size  
#print(df.columns)

df = df.drop(columns=['index'])
df = df.rename(columns = {"日誌":"log", "日期 & 時間":"time", "IP 位址":"IP", "使用者":"user", "事件":"event", "檔案/資料夾":"type", "檔案大小":"size", "fileSrcPath":"src_path"})

#Remove the slash(/)#
list_new_employee_id = []
for i in df["user"]:
    first_string, second_string = i.split('\\')
    new_employee_id = second_string
    list_new_employee_id.append(new_employee_id)
df["user"] = list_new_employee_id

list_user = df["user"].tolist()
set_user = set(list_user)

#Split user#
for user_name in set_user:
    print(user_name)
    
    tmp_df = df[df["user"]==user_name]
    
    save_csv_name = './test_data/'+'KH'+ user_name + ".csv"     #"_division_file_server_access_log.csv"
    tmp_df.to_csv(save_csv_name,index = True, encoding ='utf-8-sig')
    print("finish!!!")
    print()

