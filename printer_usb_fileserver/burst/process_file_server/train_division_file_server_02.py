# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 22:51:33 2019

@author: jia-ming
"""

########read_csv########
import pandas as pd
from os import walk

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

switch = 1 # 0:hand process 1:groupby process

read_path = "../train_log_file_server/"
save_path = "./tmp_data/" 

hand_extract_daytime_star = "2018-12-14 00:00:00"
hand_extract_daytime_end = "2019-02-04 23:59:59"

list_original_data_file_name = []

for path, folder, files in walk(read_path):
    for file_name in files:
        list_original_data_file_name.append(file_name)

for file_name in list_original_data_file_name:
    print(file_name)    
    
    df = pd.read_csv(read_path+file_name,encoding = "utf-8",engine ="c",error_bad_lines=False,warn_bad_lines=True) #,chunksize=read_size  
    extract_employee_id = df['user'].tolist()[0]
    
    #list_new_employee_id = []
    #for i in df["user"]:
    #    first_string, second_string = i.split('\\')
    #    new_employee_id = second_string
    #    list_new_employee_id.append(new_employee_id)
    #df["user"] = list_new_employee_id
    
    #df = df[df['user']==extract_employee_id]
    
    #list_new_path = []
    #for i in df['path']:
    #    separate_string =  i.split('/')
    #    final_index = len(separate_string)-1
    #    del separate_string[final_index]
    #    new_separate_string = '/'.join(separate_string)
    #    list_new_path.append(new_separate_string)
    #df['path'] =  list_new_path
    
    #df = df.rename(columns = {"security":"file_name","extention":"security_label","filename":"deputy_file_name"})
    #df = df.drop(columns=['organization','source'])
    
    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time")
    
    if switch == 0:
        
        hand_extract_df= df[hand_extract_daytime_star:hand_extract_daytime_end]
        save_csv_name = save_path + extract_employee_id + "_division_file_server_access_log.csv"
        hand_extract_df.to_csv(save_csv_name,index = True, encoding ='utf-8-sig')
        print("finish!!!")
        
    elif switch == 1:
    
        big_group = df.groupby([df.index.year,df.index.month])#,df.index.hour,df.index.minute
        for date,small_group in big_group:
            print(date)
            year = str(date[0])
            month = str(date[1])
        
            save_csv_name = save_path + extract_employee_id + '_' + year +'_'+ month +'_' + "_division_file_server_access_log.csv"
            small_group.to_csv(save_csv_name,index = True, encoding ='utf-8-sig')
            
        print("finish!!!")
            
    print()
        
