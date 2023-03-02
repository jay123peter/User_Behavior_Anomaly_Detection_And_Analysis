# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 14:44:28 2019

@author: DUNCAN
"""

##########################################
#printer burst test and train data merage#
##########################################

import pandas as pd
from os import walk

train_path = "../train_log_printer/"
test_path = "../test_log_printer/"
save_path= "../merge_printer/"

list_train_file_name = []
list_test_file_name = []

for path, folder, files in walk(train_path):
    for file_name in files:
        list_train_file_name.append(file_name)

for path, folder, files in walk(test_path):
    for file_name in files:
        list_test_file_name.append(file_name)
        
#test and train intersection file name#
set_train_file_name = set(list_train_file_name)
set_test_file_name = set(list_test_file_name)
list_file_name_intersection = list(set_train_file_name.intersection(set_test_file_name))

print("train data total user:",len(list_train_file_name))
print("test data: total user:",len(list_test_file_name))
print("totol user:",len(list_file_name_intersection))

#merge test and train data#
for file_name in list_file_name_intersection: 
    print(file_name)
    print()
    
    now_user_name = file_name.split('.')[0]

    train_df = pd.read_csv(train_path + file_name, encoding = "utf-8-sig")
    test_df = pd.read_csv(test_path + file_name, encoding = "utf-8-sig")
    
    #append#
    merge_df = train_df.append(test_df,sort=False)
    
    #sort#
    merge_df = merge_df.sort_values(by=['Time'], ascending=True)

    save_name = save_path + now_user_name + ".csv"
    merge_df.to_csv(save_name,index = False, encoding ='utf-8')

