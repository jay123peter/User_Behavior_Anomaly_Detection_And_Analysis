# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 21:15:40 2019

@author: DUNCAN
"""

##############################################
#########file server to usb  extract##########
##############################################

import pandas as pd
import datetime
from os import walk
from collections import Counter

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

################################main################################

file_server_path = "../merge_file_server/"
usb_path = "../merge_usb/"
save_path = "../extract_file_server/"

list_train_file_server = []
list_train_printer = []

for path, folder, files in walk(file_server_path):
    for file_name in files:
        list_train_file_server.append(file_name)

for path, folder, files in walk(usb_path):
    for file_name in files:
        list_train_printer.append(file_name)

#usb data and file server data intersection file name#
set_train_file_serve = set(list_train_file_server)
set_train_printer = set(list_train_printer)
list_file_name_intersection = list(set_train_file_serve.intersection(set_train_printer))

print("file server data total user:",len(list_train_file_server))
print("printer data: total user:",len(list_train_printer))
print("totol user:",len(list_file_name_intersection))
print()

################################extract feature################################

for file_name in list_file_name_intersection: 
    print(file_name)
    print()

    list_all_sum = []
    list_security_A=[]
    list_security_B=[]
    list_security_C=[]
    list_security_D=[]
    list_security_none=[]
    list_nowday_datetime = []

    df = pd.read_csv(file_server_path + file_name, encoding = "utf-8",engine ="c",error_bad_lines=False,warn_bad_lines=True,low_memory=False)

    #only calculation read data and move data#
    df = df[(df['event']=='read')|(df['event']=='move')]

    now_user_name = file_name.split('.')[0]
    df['time'] = pd.to_datetime(df["time"])
    df = df.set_index("time")

    #calculation each hour#
    big_group = df.groupby([df.index.year,df.index.month,df.index.day,df.index.hour])#,df.index.minute
    for date,small_group in big_group:
   
        #time#
        tmp_datetime = datetime.datetime(date[0],date[1],date[2],date[3])
        list_nowday_datetime.append(tmp_datetime) 
             
        ####security label#####
        security_label_count = Counter(small_group["security"])
        
        #count security A#
        security_A_count = security_label_count["Security A"]
        list_security_A.append(security_A_count)
        
        #count security B#
        security_B_count = security_label_count["Security B"]
        list_security_B.append(security_B_count)
        
        #count security C#
        security_C_count = security_label_count["Security C"]
        list_security_C.append(security_C_count)
        
        #count security D#
        security_D_count = security_label_count["Security D"]
        list_security_D.append(security_D_count)
            
        #count security none#
        security_none_count = security_label_count["none"]
        list_security_none.append(security_none_count)
        
        tmp_sum = security_A_count + security_B_count + security_C_count + security_D_count + security_none_count
        list_all_sum.append(tmp_sum)
    
    #save to dataframe#
    security_A_df = pd.DataFrame(list_security_A,columns = ['security_A'])
    security_B_df = pd.DataFrame(list_security_B,columns = ['security_B'])
    security_C_df = pd.DataFrame(list_security_C,columns = ['security_C'])
    security_D_df = pd.DataFrame(list_security_D,columns = ['security_D'])
    security_none_df = pd.DataFrame(list_security_none,columns = ['security_none'])
    sum_df = pd.DataFrame(list_all_sum,columns = ['sum'])
    all_datetime = pd.DataFrame(list_nowday_datetime,columns = ['datetime'])
    
    frames = [all_datetime,security_A_df,security_B_df,security_C_df,security_D_df,security_none_df,sum_df]
    tmp_all = pd.concat(frames,axis=1)

    save_name = save_path + now_user_name + ".csv"
    tmp_all.to_csv(save_name,index = False, encoding ='utf-8')
 





