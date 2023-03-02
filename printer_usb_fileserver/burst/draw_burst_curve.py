# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 17:32:20 2019

@author: DUNCAN
"""

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

df = pd.read_csv( "KHd5234.csv" , encoding = "utf-8-sig", low_memory=False)

df["datetime"] = pd.to_datetime(df["datetime"])

list_security = df['security_A'].tolist()
list_all = df['sum'].tolist()
list_datetime = df['datetime'].tolist()


plt.figure(figsize = (10,12))

plt.plot(list_datetime,list_all,linestyle="-",color='blue')
plt.plot(list_datetime,list_security,linestyle="-",color='red')

plt.title("r/d")
plt.ylabel('each hour use frequency')

plt.xlabel('Time')
plt.xticks(rotation=90)
plt.grid()
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))

save_name = 'C:\\Users\\DUNCAN\\Desktop\\test.png'
plt.savefig(save_name)
plt.show()


#draw curve#
#r_A = numpy_file_server_Security_A_r/numpy_file_server_d
#r_B = numpy_file_server_Security_B_r/numpy_file_server_d
#r_C = numpy_file_server_Security_C_r/numpy_file_server_d
#r_D = numpy_file_server_Security_D_r/numpy_file_server_d
#r_none = numpy_file_server_Security_none_r/numpy_file_server_d

#plt.plot(list_fileserver_datetime,r_A)
#plt.plot(list_fileserver_datetime,r_B)
#plt.plot(list_fileserver_datetime,r_C)
#plt.plot(list_fileserver_datetime,r_D)
#plt.plot(list_fileserver_datetime,r_none)

#new_list_fileserver_datetime = []
#new_file_server_Security_B_q = []
#for tmp_datatime, state in zip(list_fileserver_datetime,file_server_Security_B_q):
    #if state == 1 :
        #new_list_fileserver_datetime.append(tmp_datatime)
        #new_file_server_Security_B_q.append(state)
        

#plt.scatter(new_list_fileserver_datetime,new_file_server_Security_A_q,c='red')
#plt.scatter(new_list_fileserver_datetime,new_file_server_Security_B_q,c='red')
#plt.scatter(new_list_fileserver_datetime,new_file_server_Security_C_q,c='red')
#plt.scatter(new_list_fileserver_datetime,new_file_server_Security_D_q,c='red')
#plt.scatter(new_list_fileserver_datetime,new_file_server_Security_none_q,c='red')

#plt.xlabel('Time')
#plt.xticks(rotation=90)
#plt.grid()
#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H'))
#plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))
#plt.show()


"""

#main#
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

file_server_df["datetime"] = pd.to_datetime(file_server_df["datetime"])
list_security = file_server_df['security_B'].tolist()
list_datetime = file_server_df['datetime'].tolist()

new_list_datatime = []
new_Security_B_q = []
new_r = []
for i , t , r in zip(file_server_Security_B_q,list_datetime,numpy_file_server_Security_B_r):
    if i == 1:
        new_list_datatime.append(t)
        new_r.append(r)
        
print(new_r)
print(new_list_datatime)
   

plt.figure(figsize = (10,12))

plt.plot(list_datetime,list_security,linestyle="-",color='blue',label='curve')
plt.scatter(new_list_datatime, new_r, color='black',label='burst')


plt.title("security_B")
plt.ylabel('each hour use frequency')

plt.xlabel('Time')
plt.xticks(rotation=90)
plt.grid()
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))
plt.legend()

save_name = 'C:\\Users\\DUNCAN\\Desktop\\test.png'
plt.savefig(save_name)
plt.show()
"""