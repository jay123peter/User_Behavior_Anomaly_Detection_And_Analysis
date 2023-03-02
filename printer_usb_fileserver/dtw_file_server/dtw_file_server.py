# -*- coding: utf-8 -*-
"""
Created on Thu May 16 13:37:06 2019

@author: DUNCAN
"""

 
###anomaly detection or outlier detection###
### mode:day by day ###

def preprocess(list_file_name,data_path):
    
    tmp_file_date = []
    tmp_week_day = [] 
    all_tmp_data_list = []
    
    for file_name in list_file_name:

        read_path = data_path + file_name
        tmp_df = pd.read_csv(read_path, encoding = "utf-8")
        
        #save datetime
        tmp_df["date"] = pd.to_datetime(tmp_df["date"])
        tmp_date = tmp_df['date'][0].date()
        tmp_file_date.append(tmp_date)
        tmp_weekday = str(tmp_date.weekday()+1)
        #tmp_week_day.append(tmp_weekday)
        tmp_df = tmp_df.set_index("date")
        
        if (tmp_weekday == "6") | (tmp_weekday == "7"):
            tmp_week_day.append("67")
        else:
            tmp_week_day.append("15")
        
        ###read###
        tmp_data = tmp_df['total_read']
        tmp = tmp_data.values
        Moving_Average_list = Moving_Average(tmp,window_size)
        Moving_Average_array = np.asarray(Moving_Average_list)
        all_tmp_data_list.append(Moving_Average_array)
            
        ###zscore###
        #from scipy import stats
        #tmp_zscore = stats.zscore(Moving_Average_list)
        #zscore_list.append(tmp_zscore)
        
    return all_tmp_data_list,tmp_file_date,tmp_week_day

def generate_train_test_df(all_tmp_data_list,tmp_file_date,tmp_week_day):
    
    dataframe = pd.DataFrame(all_tmp_data_list)
    tmp_file_date = pd.DataFrame(tmp_file_date,columns=['file_date'])
    tmp_week_day = pd.DataFrame(tmp_week_day,columns=['week_day'])
    frames = [dataframe,tmp_file_date,tmp_week_day]
    tmp_dataframe = pd.concat(frames,axis=1)
    
    return tmp_dataframe


def generate_train_tree_figure(tmp_week_day_set,tmp_dataframe):
 
    tmp_list_distance = []
    tmp_dict_list_distance = {}
    tmp_dict_linkage_matrix = {}
    
    for index in tmp_week_day_set:
        print(index)
   
        new_tmp_dataframe = tmp_dataframe[tmp_dataframe.week_day == index]
        tmp_filedate = new_tmp_dataframe.pop("file_date")
        list_file_date = tmp_filedate.tolist()
        
        tmp_drop_dataframe = new_tmp_dataframe.drop(['week_day'], axis=1)
        tmp_data = tmp_drop_dataframe.values
        
        if control_tree == 0 :
            #LinkageTree
            model = clustering.LinkageTree(dists_fun=dtw.distance_matrix_fast, dists_options=option)
            linkage_matrix = model.fit(tmp_data)
        
        elif control_tree ==1:
            #HierarchicalTree
            model2 = clustering.HierarchicalTree(dists_fun=dtw.distance_matrix_fast, dists_options=option)
            linkage_matrix = model2.fit(tmp_data)
        
        #save dict linkage_matrix
        tmp_dict_linkage_matrix[index] = linkage_matrix
        
        #save dict distance
        for i in linkage_matrix:
            tmp_list_distance.append(i[2])
        tmp_dict_list_distance[index] = tmp_list_distance
        tmp_list_distance=[]
        
        fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(10, 10))
        if control_tree == 0:
            save_name = "LinkageTree("+index+").png"
        elif control_tree ==1:
            save_name = "HierarchicalTree("+index+").png"
            
        model.plot(save_name, axes=ax, show_ts_label=list_file_date,
                   show_tr_label=True, ts_label_margin=-10,
                   ts_left_margin=10, ts_sample_length=1)
        
    return tmp_dict_list_distance,tmp_dict_linkage_matrix

def generate_test_tree_figure(tmp_week_day_set, tmp_dataframe, switch):
    
    tmp_list_distance = []
    tmp_dict_list_distance = {}
    tmp_dict_linkage_matrix = {}
    
    for index in tmp_week_day_set:
        print(index)
        
        new_tmp_dataframe = tmp_dataframe[tmp_dataframe.week_day == index]
        tmp_filedate = new_tmp_dataframe.pop("file_date")
        list_file_date = tmp_filedate.tolist()
        file_date_index = len(list_file_date)-1
        test_file_date = list_file_date[file_date_index]
        test_file_date = test_file_date.strftime("%Y-%m-%d")      
        
        tmp_drop_dataframe = new_tmp_dataframe.drop(['week_day'], axis=1)
        tmp_data = tmp_drop_dataframe.values
    
        if control_tree == 0 :
            #LinkageTree
            model = clustering.LinkageTree(dists_fun=dtw.distance_matrix_fast, dists_options=option)
            linkage_matrix = model.fit(tmp_data)
        
        elif control_tree ==1:
            #HierarchicalTree
            model2 = clustering.HierarchicalTree(dists_fun=dtw.distance_matrix_fast, dists_options=option)
            linkage_matrix = model2.fit(tmp_data)
        
        #save linkage_matrix
        tmp_dict_linkage_matrix[index] = linkage_matrix
        
        #distance
        for i in linkage_matrix:
            tmp_list_distance.append(i[2])
        tmp_dict_list_distance[index] = tmp_list_distance
        tmp_list_distance=[]
            
        fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(10, 10))
        if control_tree == 0:
            save_name = "LinkageTree("+index+").png"
        elif control_tree ==1:
            save_name = "HierarchicalTree("+index+").png"
            
        model.plot(save_name, axes=ax, show_ts_label=list_file_date,
                   show_tr_label=True, ts_label_margin=-10,
                   ts_left_margin=10, ts_sample_length=1)
        
        #generate datetime list
        total_periods = tmp_data.shape[1]
        tmp_start_date = test_file_date + " 00:00:00"
        
        tmp_datetimelist = pd.date_range(start= tmp_start_date, periods=total_periods ,freq='1min')
        
        #moving average#
        plt.figure(figsize = (10,10))
        for data, date in zip(tmp_data,list_file_date):
            plt.plot(tmp_datetimelist,data,label=date,linestyle="-")
            plt.title("read moving average")
            plt.ylabel('read count')

        plt.xlabel('Time')
        plt.xticks(rotation=90)
        plt.legend()
        plt.grid()
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.gca().xaxis.set_major_locator(mdates.HourLocator())

        #save_name = 'C:\\Users\\jia-ming\\Desktop\\'+test_file_date+'-'+'-final_moving_average('+str(index)+').png'
        save_name = 'C:\\Users\\DUNCAN\\Desktop\\'+test_file_date+'-'+'-final_moving_average('+str(index)+').png'
        plt.savefig(save_name)
        plt.show()
        
    return tmp_dict_list_distance,tmp_dict_linkage_matrix


def Moving_Average(mylist,N):
    
    cumsum, moving_aves = [0], []
    for index, item in enumerate(mylist, 1):
        cumsum.append(cumsum[index-1] + item)
        if index>=N:
            moving_ave = (cumsum[index] - cumsum[index-N])/N
            moving_aves.append(moving_ave)
    return moving_aves

from os import walk
import pandas as pd
import numpy as np
from dtaidistance import dtw
from dtaidistance import clustering
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

control_tree = 0
window_size = 2
option = {'window':120}

#####################train part#####################

train_data_path ="../dtw_file_server/"
train_file_name = []

for path, folder, files in walk(train_data_path):
    for file_name in files:
        train_file_name.append(file_name)

train_file_name.remove("dtw_file_server.py")

all_train_data_list, train_file_date, train_week_day = preprocess(train_file_name,train_data_path)
train_dataframe = generate_train_test_df(all_train_data_list, train_file_date, train_week_day)
train_week_day_set = set(train_week_day)
train_week_day_set = sorted(train_week_day_set)
train_dict_list_distance,train_dict_linkage_matrix = generate_train_tree_figure(train_week_day_set,train_dataframe)


#####################test part#####################
"""
test_data_path = "../anomaly/"
test_file_name = []

for path, folder, files in walk(test_data_path):
    for file_name in files:
        test_file_name.append(file_name)

all_test_data_list, test_file_date, test_week_day = preprocess(test_file_name,test_data_path)
test_dataframe = generate_train_test_df(all_test_data_list, test_file_date, test_week_day)
test_week_day_set = set(test_week_day)
test_week_day_set = sorted(test_week_day_set)

frames = [train_dataframe,test_dataframe]
all_dataframe = pd.concat(frames,axis=0)

test_dict_list_distance, test_dict_linkage_matrix = generate_test_tree_figure(test_week_day_set, all_dataframe, control_switch)
"""


