# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 17:44:22 2019

@author: DUNCAN
"""

###########################################
###file server burst test data division####
###########################################

import pandas as pd

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

df = pd.read_csv("190704_filexferlog_UUID_UTF8_(Security B).txt",encoding = "utf-8",delimiter = ",",engine ="c",error_bad_lines=False,warn_bad_lines=True) #,chunksize=read_size  
replace_values = {"#VALUE!":"none"}
df = df.replace(replace_values)
df = df.drop(columns=['index'])
df = df.rename(columns = {"日誌":"log", "日期 & 時間":"time", "IP 位址":"IP", "使用者":"user", "事件":"event", "檔案/資料夾":"type", "檔案大小":"size", "fileSrcPath":"src_path"})

df["SecurityLB"] = df["SecurityLB"].fillna("none")
df["fileExt"] = df["fileExt"].fillna("none")

#user name Uppercase to lowercase#
list_new_employee_id = []
for i in df["user"]:
    first_string, second_string = i.split('\\')
    new_employee_id = second_string
    list_new_employee_id.append(new_employee_id)
df["user"] = list_new_employee_id

#split user#
list_user = df["user"].tolist()
set_user = set(list_user)
for user_name in set_user:
    print(user_name)
    
    tmp_df = df[df["user"]==user_name]
    
    save_csv_name = '../test_log_file_server/'+ 'KH' + user_name + ".csv"
    tmp_df.to_csv(save_csv_name,index = True, encoding ='utf-8-sig')
    print("finish!!!")
    print()
