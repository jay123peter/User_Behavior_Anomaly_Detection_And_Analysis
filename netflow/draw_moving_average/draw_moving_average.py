# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:53:35 2019

@author: DUNCAN
"""
from os import walk
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

switch = 1

moving_average_window_size = 5

#Moving_Average#
def Moving_Average(mylist,N):
    cumsum, moving_aves = [0], []
    for index, item in enumerate(mylist, 1):
        cumsum.append(cumsum[index-1] + item)
        if index>=N:
            moving_ave = (cumsum[index] - cumsum[index-N])/N
            moving_aves.append(moving_ave)
    return moving_aves


####################################
data_path ="../draw_moving_average/"

tmp_list_file_name = []

for path, folder, files in walk(data_path):
    for file_name in files:
        tmp_list_file_name.append(file_name)

tmp_list_file_name.remove("draw_moving_average.py")

tmp_file_date = [] 
tmp_list_data =[]

new_tmp_file_date = []
    
for file_name in tmp_list_file_name:
    
    read_path = data_path + file_name
    df = pd.read_csv(read_path, encoding = "utf-8")
    
    df["date"] = pd.to_datetime(df["date"])
    tmp_date = df['date'][0].date()
    tmp_file_date.append(tmp_date)
    tmp_weekday = str(tmp_date.weekday()+1)
    
    
   # tmp_file_date[0] = "test"


        

    tmp_data = df['total_bytes']
    tmp = tmp_data.values
    Moving_Average_list = Moving_Average(tmp,moving_average_window_size)
    Moving_Average_array = np.asarray(Moving_Average_list)
    tmp_list_data.append(Moving_Average_array)


    total_periods = 1436
    tmp_start_date = tmp_file_date[0]
    
    tmp_datetimelist = pd.date_range(start= tmp_start_date, periods=total_periods ,freq='1min')

    #moving average#
    plt.figure(figsize = (10,10))
    for data, date in zip(tmp_list_data,tmp_file_date):
        plt.plot(tmp_datetimelist,data,label=date,linestyle="-")
    
    if switch == 0:
        plt.title("link moving average")
        plt.ylabel('link count')
    elif switch == 1:
        plt.title("Tuesday flow moving average")
        plt.ylabel('bytes')
        
    plt.xlabel('Time')
    plt.xticks(rotation=90)
    plt.legend()
    plt.grid()
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator())
    
    save_name = 'C:\\Users\\DUNCAN\\Desktop\\final_moving_average.png'
    plt.savefig(save_name)
    plt.show()
