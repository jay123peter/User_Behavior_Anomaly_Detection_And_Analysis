# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 15:51:42 2019

@author: DUNCAN
"""


########################################
###printer burst test data division#####
########################################

import pandas as pd

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

list_new_user_name = []

read_path = 'Printer Log_20190704_UUID_UTF8_(Security B).txt'

df = pd.read_csv(read_path, encoding ="utf-8", sep="," , engine ="c", error_bad_lines=False, warn_bad_lines=True)

df = df.drop(columns=["index","Grayscale","Height","Language","Paper_Size","Printer","PrintServer","Width","_time"])
replace_values = {"#VALUE!":"none"}
df = df.replace(replace_values)

df["SecurityLB"] = df["SecurityLB"].fillna("none")
df["fileExt"] = df["fileExt"].fillna("none")
list_user_name = df["User"].tolist()

#user name Uppercase to lowercase#
for user_name in list_user_name:
    user_name = str(user_name)
    list_new_user_name.append(user_name.lower())
df["User"] = list_new_user_name

#split user#
list_new_user_name = df["User"].tolist()
set_user = set(list_new_user_name)

for user_name in set_user:
    print(user_name)
    
    filter_df = df[df['User']==user_name]
    save_name = '../test_log_printer/'+ 'KH' + user_name + ".csv"

    filter_df.to_csv(save_name,index = False, encoding ='utf-8-sig')
    print("finish!!!")
    print()


