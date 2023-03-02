# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 16:43:45 2019

@author: DUNCAN
"""

########################################
#####usb burst test data division#######
########################################

import pandas as pd


#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

read_path = 'USB Log_20190704_UUID_UTF8_(Security B).txt'
df = pd.read_csv(read_path, encoding = "utf-8", delimiter=',', error_bad_lines=False, warn_bad_lines=True)#, low_memory=False    #utf-8 big5#,chunksize=read_size

#print(df.columns)

df = df.drop(columns=['index','AGT_GRP_NAME', 'AGT_NAME', 'UD_APP_NAME', 'UD_APP_TITLE', 'UD_CLS_NAME', 'UD_DESC', 'UD_FILE_TIME', 'UD_LABEL', 'UD_TYPE', 'UD_UDISK_SIZE', 'UD_VOLUME_SN', '_time'])#,"UD_LABEL"
df = df.dropna(subset=['USR_NAME','fileSrcPath'])

#df = df[df.UD_SUBTYPE.isin(['複製出']) | df.UD_SUBTYPE.isin(['移動出'])]
#replace_dict = {'連接': 'connect', '複製出': "copy_in", '新增磁碟': 'add_disk', '移除磁碟': 'remove_disk', '刪除': 'delete', '移動出': 'move_in', '移動到': 'move_out', '複製到': 'copy_out', '上傳': 'upload', 'UD_SUBTYPE': 'none', '重新命名': 'rename', '新增檔案': 'add_file', '修改': 'modify'}
#df = df.replace(replace_dict)#, inplace=True

df["SecurityLB"] = df["SecurityLB"].fillna("none")
#df["fileSrcPath"] = df["fileSrcPath"].fillna("none")

#remove Slash(/)#
list_user_name = []
for i in df['USR_NAME']:
    tmp_string = i.split('\\')
    list_user_name.append(tmp_string[1])
df['USR_NAME'] = list_user_name

list_user_name = df['USR_NAME'].tolist()

#user name uppercase to lowercase#
list_new_user_name =[]
for user_name in list_user_name:
    user_name = str(user_name)
    list_new_user_name.append(user_name.lower())
df["USR_NAME"] = list_new_user_name

list_user_name = df['USR_NAME'].tolist()
set_user_name = set(list_user_name)

#Split user#
for user_name in set_user_name:
    print(user_name)
    extract_employee_id = str(user_name)
    
    filter_df = df[df['USR_NAME']==extract_employee_id]
    
    save_name = "../test_log_usb/"+"KH" +extract_employee_id + ".csv"
    filter_df = filter_df.sort_values(by='UD_TIME', ascending=True)
    filter_df.to_csv(save_name,index = False, encoding ='utf_8_sig')
    print("finish!!!")
    print()
