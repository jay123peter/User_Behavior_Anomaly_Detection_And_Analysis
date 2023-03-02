# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:53:35 2019

@author: DUNCAN
"""

########################################
#######zscore detect abnormality########
########################################


from os import walk
import datetime
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter

control_percetile_switch = 1
control_percetile_day = 60  #control_day 
control_count = 10          #usage count
switch = 0                  #0:all,    1:hot path,    2:cold path
cold_path_threshold_1 = 1   #Threshold 1 < zscore < Threshold 2 warning
cold_path_threshold_2 = 2   #zscore > Threshold 2 abnormal

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

train_path = './train_data/'
test_path = './test_data/'    

list_train_file_name = []
list_test_file_name = []
all_df = pd.DataFrame(columns=['user','day','path','access count','zscore_state','zscore','description','total_data','remark'])

#check parent child path relationship#
def check_parent_child(test_path ,list_train_path):
    test_path = Path(test_path)
    
    for tmp_train_path in list_train_path:
        train_path = Path(tmp_train_path)
        
        if train_path in test_path.parents:
            
            str_train_path = str(train_path)
            divsion_str_train_path = str_train_path.split('\\')
            modify_str_train_path ='/'.join(divsion_str_train_path)
            
            return True,modify_str_train_path
            
    return False,None

#Remove the head and tail slash(/)#
def modify_path(tmp_df):
    list_modify_path = []
    for tmp_path in tmp_df['src_path']:
        divsion_tmp_path = tmp_path.split('/')
        divsion_tmp_path = list(filter(None,divsion_tmp_path))
        modify_tmp_path ='/'.join(divsion_tmp_path)
        list_modify_path.append(modify_tmp_path)
    tmp_df = tmp_df.drop(columns=['src_path'])
    tmp_df['src_path'] = list_modify_path.copy()
    return tmp_df

#############train part#############

for path, folder, files in walk(train_path):
    for file_name in files:
        list_train_file_name.append(file_name)

for path, folder, files in walk(test_path):
    for file_name in files:
        list_test_file_name.append(file_name)
        
set_train_file_name = set(list_train_file_name)
set_test_file_name = set(list_test_file_name)
list_file_name_intersection = list(set_train_file_name.intersection(set_test_file_name))

print("train data total user:",len(list_train_file_name))
print("test data: total user:",len(list_test_file_name))
print("totol user:",len(list_file_name_intersection))


for file_name in list_file_name_intersection:
    print(file_name)

    train_df = pd.read_csv( train_path + file_name , encoding = "utf-8-sig", low_memory=False)
    train_df.reset_index(level=0, inplace=True)
    train_df = train_df.dropna(subset=['src_path'])
    
    train_df = train_df[(train_df['event']=='read')|(train_df['event']=='move')]
    train_df = modify_path(train_df)
    
    ################## frequency count ######################

    list_path = train_df['src_path'].tolist()
    frequency_count_path = Counter(list_path) 
    
    ################### Number of days used ######################
    
    train_df['time'] = pd.to_datetime(train_df['time'])

    ###all day###
    head_df = train_df.head(1)
    start_time = head_df['time'].tolist()[0]

    tail_df = train_df.tail(1)
    end_time = tail_df['time'].tolist()[0]
    all_day = end_time-start_time
    all_day = all_day.days
    #print(end_time)
    #print(start_time)
    #print(all_day)

    train_df = train_df.set_index('time')

    use_day_count_path = Counter()
    train_dict_each_day_count = {}
    
    #each day path#
    big_group = train_df.groupby([train_df.index.year,train_df.index.month,train_df.index.day])
    for date,small_group in big_group:
        print(date)
        
        tmp_train_path = small_group['src_path'].tolist()
        train_each_day_count_path = Counter(tmp_train_path)
        
        #save to dict (each_day_count)#
        set_train_path = set(tmp_train_path)
        for key in set_train_path :
            
            train_dict_each_day_count.setdefault(key, [])
            train_dict_each_day_count[key].append(train_each_day_count_path[key])
        
        #Number of days used#
        use_day_count_path.update(set_train_path)
    
    list_train_path = train_df['src_path'].tolist()
    set_train_path = set(list_train_path)
    
    list_train_path_name = []
    list_train_use_day = []
    list_train_use_count = []
    
    #save to dataframe#
    for i in set_train_path:
        
        list_train_path_name.append(i)
        
        #frequency count#
        path_count = frequency_count_path[i]
        list_train_use_count.append(path_count)
        
        #use_day_count#
        path_use_day = use_day_count_path[i]
        list_train_use_day.append(path_use_day)
    
    train_df_path = pd.DataFrame(list_train_path_name, columns = ['path'])
    train_df_use_day = pd.DataFrame(list_train_use_day, columns = ['use_day'])
    train_df_use_count = pd.DataFrame(list_train_use_count, columns = ['access count'])
    
    frames = [train_df_path,train_df_use_day,train_df_use_count]
    train_df_all = pd.concat(frames,axis = 1)
    
    train_df_all = train_df_all.sort_values(by='use_day', ascending=False)
    #train_df_all.to_csv('train_count_path.csv',index = False, encoding = 'utf-8-sig')
    
    #choose use day count method#
    if control_percetile_switch == 0:
        use_day_precentile = np.percentile(list_train_use_day, control_percetile_day)
    elif control_percetile_switch == 1:
        use_day_precentile = round(all_day * control_percetile_day/100 +1)

    ############# division hot path cold path #############
    
    #hot path#
    train_df_all['use_day_big']  = train_df_all['use_day'] >= use_day_precentile
    train_df_all['use_count_big']  = train_df_all['access count'] >= control_count
    
    train_df_hot_path = train_df_all[ (train_df_all['use_day_big'] == True) & (train_df_all['use_count_big'] == True)]
    list_hot_path = train_df_hot_path['path'].tolist()
    set_hot_path = set(list_hot_path)
    
    ############# train hot mean stdev #############
    #list_train_hot_path_count_mean = []
    #list_train_hot_path_count_stdev = []
    
    #for i in set_hot_path:
        
        #mean#
        #values = train_dict_each_day_count[i]        
        #tmp_mean = np.mean(values)
        #list_train_hot_path_count_mean.append(tmp_mean)
        
        #stdev#
        #tmp_stdev = np.std(values)
        #list_train_hot_path_count_stdev.append(tmp_stdev)
    
    #save to dataframe#
    #df_train_hot_path_name = pd.DataFrame(list_hot_path, columns = ['path'])
    #df_train_hot_path_mean = pd.DataFrame(list_train_hot_path_count_mean, columns = ['mean'])
    #df_train_hot_path_stdev = pd.DataFrame(list_train_hot_path_count_stdev, columns = ['stdev'])
    
    #frames = [df_train_hot_path_name,df_train_hot_path_mean,df_train_hot_path_stdev]
    #df_train_hot_path_all = pd.concat(frames,axis = 1)
    #df_train_hot_path_all.to_csv('df_train_hot_path_all.csv',index = False, encoding = 'utf-8-sig')
    
    #cold path(not in hot path set)#
    list_true_or_false = []
    for i in train_df_all['path']:
        list_true_or_false.append(i not in set_hot_path)
    
    ############# division cold path cold path #############
    train_df_cold_path = train_df_all[list_true_or_false]
    list_cold_path = train_df_cold_path['path'].tolist()
    set_cold_path = set(list_cold_path)
    
    ############# train cold path mean stdev #############
    list_train_cold_path_count_mean = []
    list_train_cold_path_count_stdev = []
    list_train_cold_path_len = [] 
    
    for i in list_cold_path:
        
        #count#
        values = train_dict_each_day_count[i]
        tmp_len = len(values)
        list_train_cold_path_len.append(tmp_len)
        
        #mean#
        tmp_mean = np.mean(values)
        list_train_cold_path_count_mean.append(tmp_mean)
        
        #stdev#
        tmp_stdev = np.std(values)
        list_train_cold_path_count_stdev.append(tmp_stdev)
        
    
    #save to dataframe#
    df_train_cold_path_name = pd.DataFrame(list_cold_path, columns = ['path'])
    df_train_cold_path_mean = pd.DataFrame(list_train_cold_path_count_mean, columns = ['mean'])
    df_train_cold_path_stdev = pd.DataFrame(list_train_cold_path_count_stdev, columns = ['stdev'])
    df_train_cold_path_len = pd.DataFrame(list_train_cold_path_len, columns = ['total data'])
    
    frames = [df_train_cold_path_name,df_train_cold_path_mean,df_train_cold_path_stdev,df_train_cold_path_len]
    df_train_cold_path_all = pd.concat(frames,axis = 1)
    #df_train_cold_path_all.to_csv('df_train_cold_path_all.csv',index = False, encoding = 'utf-8-sig')

    #df_each_day_count = pd.DataFrame.from_dict(train_dict_each_day_count, orient='index')
    #df_each_day_count.to_csv('df_each_day_count.csv',index = True, encoding = 'utf-8-sig')

#############test part#############
    
    list_zscore_result = []
    list_test_zscore = []
    list_test_path_name = []
    list_test_use_count = []
    list_test_day = []
    list_user_name = []
    list_description = []
    list_new_src_path = []
    list_total_data = []
    list_remark = []
    
    list_train_cold_path = train_df_cold_path['path'].tolist()
    list_train_hot_path = train_df_hot_path['path'].tolist()

    test_df = pd.read_csv(test_path + file_name, encoding = "utf-8-sig", low_memory=False)
    test_df = test_df.dropna(subset=['src_path'])
    test_read_df = test_df[test_df['event']=='read'].copy()

    test_move_df = test_df[test_df['event']=='move'].copy()
    
    #remove ->#
    if test_move_df.empty:
        test_df = test_read_df
    else:
        tmp_test_src_path_list = test_move_df['src_path'].tolist()
        for i in  tmp_test_src_path_list:
            tmp_src_path = i.split('->')[0]
            list_new_src_path.append(tmp_src_path)
        test_move_df['src_path'] = list_new_src_path
        test_df = test_read_df.append(test_move_df)

    #remove the slash(/)#
    test_df = modify_path(test_df)
    
    test_df['time'] = pd.to_datetime(test_df['time'])
    test_df = test_df.set_index('time')
    
    #test data each day#
    big_group = test_df.groupby([test_df.index.year,test_df.index.month,test_df.index.day])
    for date,small_group in big_group:
        print(date)
        
        list_small_group_path = small_group['src_path'].tolist()   
        small_group_use_count = Counter(list_small_group_path)
        set_small_group_path = set(list_small_group_path)
        
        for tmp_path in set_small_group_path:
            
            division_file_name = file_name.split('.')
            first_file_name = division_file_name[0]
            list_user_name.append(first_file_name)

            tmp_time = datetime.datetime(date[0],date[1],date[2])
            list_test_day.append(tmp_time)
            list_test_path_name.append(tmp_path)
            tmp_path_count = small_group_use_count[tmp_path]
            list_test_use_count.append(tmp_path_count)
        
            #child_hot_path_truefalse,tmp_parent_hot_path = check_parent_child(tmp_path,list_train_hot_path)
            #child_cold_path_truefalse,tmp_parent_cold_path = check_parent_child(tmp_path,list_train_cold_path)
            
            #if hot path:normal#
            if tmp_path in list_train_hot_path:
                list_zscore_result.append("normal")
                list_test_zscore.append(np.nan)
                list_description.append("normal hot directory")
                list_total_data.append(np.nan)
                list_remark(np.nan)
            
            #if cold path:zscore#
            elif tmp_path in list_train_cold_path:
                
                    tmp_df_train_cold_path = df_train_cold_path_all[df_train_cold_path_all['path'] == tmp_path]
                    tmp_mean = tmp_df_train_cold_path['mean'].tolist()[0]
                    tmp_stdev = tmp_df_train_cold_path['stdev'].tolist()[0]
                    tmp_total_data = tmp_df_train_cold_path['total data'].tolist()[0]
                    
                    list_total_data.append(tmp_total_data)
                    
                    if tmp_total_data < 5:
                        list_remark("data < 5")
                    else:
                        list_remark(np.nan)
                        
                        
                    ###z-score###
                    if tmp_stdev == 0:
                        tmp_z_scores = 0
                    else:
                        tmp_z_scores = (tmp_path_count - tmp_mean) / tmp_stdev
                    list_test_zscore.append(tmp_z_scores)
    
                    if tmp_z_scores >= cold_path_threshold_2:
                        list_zscore_result.append("anomaly")
                        list_description.append("anomaly cold directory")
                        
                    elif tmp_z_scores >= cold_path_threshold_1:
                        list_zscore_result.append("warning")
                        list_description.append("warning cold directory")
                        
                    elif tmp_z_scores < cold_path_threshold_1:
                        list_zscore_result.append("normal")
                        list_description.append("normal cold directory")
            
            #first shown path#
            else:
                list_zscore_result.append("anomaly")
                list_test_zscore.append(np.nan)
                list_description.append("not shown before")
                list_total_data.append(np.nan)
                list_remark(np.nan)
    print()
    
    #save user dataframe#
    test_df_user_name = pd.DataFrame(list_user_name, columns = ['user'])
    test_df_day = pd.DataFrame(list_test_day, columns = ['day'])
    test_df_path = pd.DataFrame(list_test_path_name, columns = ['path'])
    test_df_count = pd.DataFrame(list_test_use_count, columns = ['access count'])
    test_df_zscore = pd.DataFrame(list_test_zscore, columns = ['zscore'])
    test_df_zscore_result = pd.DataFrame(list_zscore_result, columns = ['zscore_state'])
    test_df_description = pd.DataFrame(list_description, columns = ['description'])
    test_df_total_data = pd.DataFrame(list_total_data, columns = ['total_data'])
    test_df_remark = pd.DataFrame(list_remark, columns = ['remark'])

    frames = [test_df_user_name,test_df_day,test_df_path,test_df_count,test_df_zscore,test_df_zscore_result,test_df_description,test_df_total_data,test_df_remark]
    test_df_all = pd.concat(frames,axis = 1)
    test_df_all = test_df_all.sort_values(by='day', ascending=True)
    test_df_all.to_csv(first_file_name+'_test_result.csv',index = False, encoding = 'utf-8-sig')
    
    #save all user anomaly and warning#
    test_df_merge = test_df_all[(test_df_all['zscore_state'] == 'anomaly') | (test_df_all['zscore_state'] == 'warning')]
    all_df = all_df.append(test_df_merge, ignore_index=True,  sort=False)

all_df.to_csv('All_Abnormal_Users.csv',index = False, encoding = 'utf-8-sig')





