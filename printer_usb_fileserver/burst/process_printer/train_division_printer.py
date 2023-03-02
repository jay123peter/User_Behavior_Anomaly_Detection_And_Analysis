# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 16:36:01 2019

@author: DUNCAN
"""

########################################
###printer burst train data division####
########################################


import pandas as pd
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

list_new_user_name = []

read_path = './2019_GMO_PRINTER_UUID.csv'

replace_values = {"#VALUE!":"none"}    #,"NaN":"none_test"

df = pd.read_csv(read_path, encoding = "big5", engine ="c", error_bad_lines=False, warn_bad_lines=True)     #utf-8 big5#,chunksize=read_size  

df = df.drop(columns=["index","Grayscale","Height","Language","Paper_Size","Printer","PrintServer","Width","_time"])
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
for user_name in list_new_user_name:
    print(user_name)

    new_user_name = user_name.lower()
    print(new_user_name)

    filter_df = df[df['User']==new_user_name ]
    
    save_name = '../train_log_printer/' + 'KH' + new_user_name + ".csv"
    filter_df.to_csv(save_name,index = False, encoding ='utf-8')
    print("finish!!!")
    print()


