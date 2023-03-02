# -*- coding: utf-8 -*-
"""
Created on Sun Dec  2 15:56:19 2018

@author: jia-ming
"""
#####draw test#######
"""
from os import walk
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter 
import humanfriendly

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

now_my_path = "C:\\Users\\jia-ming\\Desktop\\work\\report\\user_behavior_dataset\\netfolw_7天_20181107"
#"C:\\Users\\DUNCAN\\Desktop\\work\\report\\user_behavior_dataset\\netfolw_7天_20181107"
#"C:\\Users\\jia-ming\\Desktop\\work\\report\\user_behavior_dataset\\netfolw_7天_20181107"

all_file_name = []

for path, folder, files in walk(now_my_path):
    for file_name in files:
        all_file_name.append(file_name)
all_file_name.remove("statistac.py")
all_file_name.remove("extract_feature.py")
all_file_name.remove("burst_detection.py")
all_file_name.remove("draw_picture.py")
all_file_name.remove("all_20181113.py")

for name in all_file_name:

    print(name)
    df_all = pd.read_csv(name,encoding = 'big5')
    #print(df_all.head())
    #print()
    
    destination_ip = df_all["來源IP"]
    #print(Counter(destination_ip).most_common(10))
    #print()
    
    #10.11.13.169 65114, 10.11.10.77 4417
    df_all = df_all[df_all.來源IP.isin(["10.11.13.169"]) | df_all.目的IP.isin(["10.11.13.169"])]
    #df_all = df_all[df_all.目的IP.isin(["10.11.13.169"])]
    #print(df_all.head())
    #print()
    
    data_time = df_all["時間"]
    bytes_list= df_all["Bytes"]
    
    new_bytes_list = []
    for i in bytes_list:
        new_bytes_list.append(humanfriendly.parse_size(i))
       
    data_time = pd.to_datetime(data_time)
        
    plt.plot(data_time,new_bytes_list, label = 'bytes')
    plt.xlabel('Time')
    plt.ylabel('bytes')
    plt.xticks(rotation=90)  
    plt.legend()
    plt.show()
"""

#####draw 10_11_65_169 IsolationForest######

"""
import pandas as pd
df_all = pd.read_csv("10_11_65_169_two_feature_all.csv",encoding = 'big5')
#print(df_all)
#print()

total_bytes = df_all["total_bytes"]
total_link = df_all["total_link_count"]

train_matrix = df_all.values
#print(train_matrix)
#print()

import numpy as np
from sklearn.ensemble import IsolationForest
rng = np.random.RandomState(42)
clf = IsolationForest(n_estimators=100, max_samples='auto', random_state=rng, contamination=0.1)
#clf = IsolationForest(n_estimators=100, max_samples='auto', random_state=rng, contamination = )
clf.fit(train_matrix)
y_pred_train = clf.predict(train_matrix)

from collections import Counter 
print(Counter(y_pred_train))
print()

y_pred_train = pd.DataFrame(y_pred_train,columns=["y_pred_train"])
#print(y_pred_train)
#print()

frames = [df_all,y_pred_train]
df_all = pd.concat(frames,axis=1)
#print(df_all)

df_outlier = df_all [df_all.y_pred_train.isin([-1])]
#print(df_outlier)

df_inlier = df_all [df_all.y_pred_train.isin([1])]
#print(df_inlier)

outlier_total_bytes = df_outlier.pop("total_bytes")
outlier_total_link_count = df_outlier.pop("total_link_count")
inlier_total_bytes = df_inlier.pop("total_bytes")
inlier_total_link_count = df_inlier.pop("total_link_count")

#############use log###############

#new_outlier_total_bytes = []
#new_outlier_total_link_count = []
#new_inlier_total_bytes = []
#new_inlier_total_link_count = []

#import math

#for i in outlier_total_bytes:
#    new_outlier_total_bytes.append(math.log(i))
    
#for i in outlier_total_link_count:
#    new_outlier_total_link_count.append(math.log(i))
    
#for i in inlier_total_bytes:
#    new_inlier_total_bytes.append(math.log(i))
    
#for i in inlier_total_link_count:
#    new_inlier_total_link_count.append(math.log(i))
    
#outlier_total_bytes = new_outlier_total_bytes
#outlier_total_link_count = new_outlier_total_link_count
#inlier_total_bytes = new_inlier_total_bytes
#inlier_total_link_count = new_inlier_total_link_count

############################

import matplotlib.pyplot as plt
p1 = outlier_total_bytes
p2 = outlier_total_link_count
p3 = inlier_total_bytes
p4 = inlier_total_link_count

plt.figure('Draw',figsize=(10,5))
plt.title("one week")
plt.xlabel('G Bytes')
plt.ylabel('Total link')
red_point = plt.scatter(p1, p2, c='red', alpha=0.6, label='inlier')
blue_point = plt.scatter(p3, p4, c='blue', alpha=0.6, label='outlier')
plt.legend([red_point, blue_point], ['outlier', 'inlier'], loc = 'upper right')    
plt.show()
#plt.scatter(x3, y3, s=area, c=area, marker='v', cmap='Reds', alpha=0.7)
#ax.scatter(x3, y3, s=area, alpha=0.5, linewidths=[3], edgecolors='r')
"""

#####draw 10_11_65_169_公式_1  ######
"""
import pandas as pd
import numpy as np
from statistics import mean
from collections import Counter

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

df_all = pd.read_csv("10_11_65_169_two_feature_all.csv",encoding = 'big5')
#print(df_all)
#print()

total_bytes = df_all["total_bytes"]
total_bytes_mean = mean(total_bytes)
total_bytes_std = np.std(total_bytes)
print("total_bytes_mean",total_bytes_mean)
print("total_bytes_std",total_bytes_std)
print()


total_link_count = df_all["total_link_count"]
total_link_count_mean = mean(total_link_count)
total_link_count_std = np.std(total_link_count)
print("total_link_count_mean",total_link_count_mean)
print("total_link_count_std",total_link_count_std)
print()


list_total_bytes_target = []
list_total_link_target = []

for i in total_bytes:
    if (abs(i - total_bytes_mean)-2*total_bytes_std) > 0 :
        list_total_bytes_target.append(-1)
    else:
        list_total_bytes_target.append(1)
print("total_bytes have abs:")
print(Counter(list_total_bytes_target))


for i in total_link_count:
    if (abs(i - total_link_count_mean)-2*total_link_count_std) > 0 :
        list_total_link_target.append(-1)
    else:
        list_total_link_target.append(1)
print("total_link have abs:")
print(Counter(list_total_link_target))


total_bytes_target = pd.DataFrame(list_total_bytes_target,columns=["total_bytes_target"])
total_link_target = pd.DataFrame(list_total_link_target,columns=["total_link_target"])

#print(y_pred_train)
#print()

frames = [df_all,total_link_target,total_bytes_target]
df_all = pd.concat(frames,axis=1)
#print(df_all)

df_all_link = df_all
df_all_byte = df_all

######link#######
df_link_outlier = df_all_link [df_all_link.total_link_target.isin([-1])]
#print(df_link_outlier.shape)
df_link_inlier = df_all_link [df_all_link.total_link_target.isin([1])]
#print(df_link_inlier.shape)

df_link_outlier_bytes_x = df_link_outlier.pop("total_bytes")
df_link_outlier_link_y = df_link_outlier.pop("total_link_count")
df_link_inlier_bytes_x = df_link_inlier.pop("total_bytes")
df_link_inlier_link_y = df_link_inlier.pop("total_link_count")


######byte#######
df_byte_outlier = df_all_byte [df_all_byte.total_bytes_target.isin([-1])]
#print(df_link_outlier.shape)
df_byte_inlier = df_all_byte [df_all_byte.total_bytes_target.isin([1])]
#print(df_link_inlier.shape)

df_byte_outlier_bytes_x = df_byte_outlier.pop("total_bytes")
df_byte_outlier_link_y = df_byte_outlier.pop("total_link_count")
df_byte_inlier_bytes_x = df_byte_inlier.pop("total_bytes")
df_byte_inlier_link_y = df_byte_inlier.pop("total_link_count")

######use log#######
#link#
#new_df_link_outlier_bytes_x = []
#new_df_link_outlier_link_y = []
#new_df_link_inlier_bytes_x = []
#new_df_link_inlier_link_y = []

#import math

#for i in df_link_outlier_bytes_x:
#    new_df_link_outlier_bytes_x.append(math.log(i))
    
#for i in df_link_outlier_link_y:
#    new_df_link_outlier_link_y.append(math.log(i))
    
#for i in df_link_inlier_bytes_x:
#    new_df_link_inlier_bytes_x.append(math.log(i))
    
#for i in df_link_inlier_link_y:
#    new_df_link_inlier_link_y.append(math.log(i))
    
#df_link_outlier_bytes_x = new_df_link_outlier_bytes_x
#df_link_outlier_link_y = new_df_link_outlier_link_y
#df_link_inlier_bytes_x = new_df_link_inlier_bytes_x
#df_link_inlier_link_y = new_df_link_inlier_link_y

#byte#

#new_df_byte_outlier_bytes_x = []
#new_df_byte_outlier_link_y = []
#new_df_byte_inlier_bytes_x = []
#new_df_byte_inlier_link_y = []

#import math
#for i in df_byte_outlier_bytes_x :
#    new_df_byte_outlier_bytes_x.append(math.log(i))
    
#for i in df_byte_outlier_link_y:
#    new_df_byte_outlier_link_y.append(math.log(i))
    
#for i in df_byte_inlier_bytes_x:
#    new_df_byte_inlier_bytes_x.append(math.log(i))
    
#for i in df_byte_inlier_link_y:
#    new_df_byte_inlier_link_y.append(math.log(i))
    
#df_byte_outlier_bytes_x = new_df_byte_outlier_bytes_x
#df_byte_outlier_link_y = new_df_byte_outlier_link_y
#df_byte_inlier_bytes_x = new_df_byte_inlier_bytes_x
#df_byte_inlier_link_y = new_df_byte_inlier_link_y

####################

######draw#######
#link#
import matplotlib.pyplot as plt
fig=plt.figure(figsize=(10,5))
plt.subplots_adjust(wspace =0, hspace =0.5)

link_p1_x = df_link_outlier_bytes_x
link_p2_y = df_link_outlier_link_y
link_p3_x = df_link_inlier_bytes_x
link_p4_y = df_link_inlier_link_y

plt.subplot(2,1,1)
plt.title("link anomaly")
plt.xlabel('G Bytes')
plt.ylabel('Total link')
red_point = plt.scatter(link_p1_x, link_p2_y, c='red', alpha=0.6, label='outlier')
blue_point = plt.scatter(link_p3_x, link_p4_y, c='blue', alpha=0.6, label='inlier')
plt.legend([red_point, blue_point], ['outlier', 'inlier'], loc = 'upper right')    

#bytes#
byte_p1_x = df_byte_outlier_bytes_x
byte_p2_y = df_byte_outlier_link_y
byte_p3_x = df_byte_inlier_bytes_x
byte_p4_y = df_byte_inlier_link_y

plt.subplot(2,1,2)
plt.title("flow anomaly")
plt.xlabel('G Bytes')
plt.ylabel('Total link')
red_point = plt.scatter(byte_p1_x, byte_p2_y, c='red', alpha=0.6, label='outlier')
blue_point = plt.scatter(byte_p3_x, byte_p4_y, c='blue', alpha=0.6, label='inlier')
plt.legend([red_point, blue_point], ['outlier', 'inlier'], loc = 'upper right')    
plt.show()
"""

#####draw 10_11_65_169_公式_2  ######
"""
import pandas as pd
import numpy as np
from statistics import mean
from collections import Counter

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

df_all = pd.read_csv("10_11_65_169_two_feature_all.csv",encoding = 'big5')
#print(df_all)
#print()

total_bytes = df_all["total_bytes"]
total_bytes_mean = mean(total_bytes)
total_bytes_std = np.std(total_bytes)
print("total_bytes_mean",total_bytes_mean)
print("total_bytes_std",total_bytes_std)
print()


total_link_count = df_all["total_link_count"]
total_link_count_mean = mean(total_link_count)
total_link_count_std = np.std(total_link_count)
print("total_link_count_mean",total_link_count_mean)
print("total_link_count_std",total_link_count_std)
print()


list_total_bytes_target = []
list_total_link_target = []

for i in total_bytes:
    if (abs(i - total_bytes_mean)-2*total_bytes_std) > 0 :
        list_total_bytes_target.append(-1)
    else:
        list_total_bytes_target.append(1)
print("total_bytes have abs:")
print(Counter(list_total_bytes_target))


for i in total_link_count:
    if (abs(i - total_link_count_mean)-2*total_link_count_std) > 0 :
        list_total_link_target.append(-1)
    else:
        list_total_link_target.append(1)
print("total_link have abs:")
print(Counter(list_total_link_target))


total_bytes_target = pd.DataFrame(list_total_bytes_target,columns=["total_bytes_target"])
total_link_target = pd.DataFrame(list_total_link_target,columns=["total_link_target"])
frames = [df_all,total_link_target,total_bytes_target]
df_all = pd.concat(frames,axis=1)
print(df_all)


######all#######
df_all_outlier_1 = df_all [df_all.total_bytes_target.isin([-1]) & df_all.total_link_target.isin([-1])]
df_all_outlier_2 = df_all [df_all.total_bytes_target.isin([-1]) & df_all.total_link_target.isin([1])]
df_all_outlier_3 = df_all [df_all.total_bytes_target.isin([1]) & df_all.total_link_target.isin([-1])]
df_all_inlier = df_all [df_all.total_bytes_target.isin([1]) & df_all.total_link_target.isin([1])]


df_all_outlier_1_bytes_x = df_all_outlier_1.pop("total_bytes")
df_all_outlier_1_link_y = df_all_outlier_1.pop("total_link_count")

df_all_outlier_2_bytes_x = df_all_outlier_2.pop("total_bytes")
df_all_outlier_2_link_y = df_all_outlier_2.pop("total_link_count")

df_all_outlier_3_bytes_x = df_all_outlier_3.pop("total_bytes")
df_all_outlier_3_link_y = df_all_outlier_3.pop("total_link_count")

df_all_inlier_bytes_x = df_all_inlier.pop("total_bytes")
df_all_inlier_link_y = df_all_inlier.pop("total_link_count")

######use log#######
#link#
new_df_all_outlier_1_bytes_x = []
new_df_all_outlier_1_link_y = []

new_df_all_outlier_2_bytes_x = []
new_df_all_outlier_2_link_y = []

new_df_all_outlier_3_bytes_x = []
new_df_all_outlier_3_link_y = []

new_df_all_inlier_bytes_x = []
new_df_all_inlier_link_y = []

import math
for i in df_all_outlier_1_bytes_x:
    new_df_all_outlier_1_bytes_x.append(math.log(i))
    
for i in df_all_outlier_1_link_y:
    new_df_all_outlier_1_link_y.append(math.log(i))
    
    
for i in df_all_outlier_2_bytes_x:
    new_df_all_outlier_2_bytes_x.append(math.log(i))
    
for i in df_all_outlier_2_link_y:
    new_df_all_outlier_2_link_y.append(math.log(i))
    
    
for i in df_all_outlier_3_bytes_x:
    new_df_all_outlier_3_bytes_x.append(math.log(i))
    
for i in df_all_outlier_3_link_y:
    new_df_all_outlier_3_link_y.append(math.log(i))    

    
for i in df_all_inlier_bytes_x:
    new_df_all_inlier_bytes_x.append(math.log(i))
    
for i in df_all_inlier_link_y :
    new_df_all_inlier_link_y.append(math.log(i))
    

df_all_outlier_1_bytes_x = new_df_all_outlier_1_bytes_x
df_all_outlier_1_link_y = new_df_all_outlier_1_link_y
df_all_outlier_2_bytes_x = new_df_all_outlier_2_bytes_x
df_all_outlier_2_link_y = new_df_all_outlier_2_link_y
df_all_outlier_3_bytes_x = new_df_all_outlier_3_bytes_x
df_all_outlier_3_link_y = new_df_all_outlier_3_link_y
df_all_inlier_bytes_x = new_df_all_inlier_bytes_x
df_all_inlier_link_y = new_df_all_inlier_link_y
    

####################

######draw#######
#all#
import matplotlib.pyplot as plt

outlier_1_bytes_x = df_all_outlier_1_bytes_x
outlier_1_link_y = df_all_outlier_1_link_y

outlier_2_bytes_x = df_all_outlier_2_bytes_x
outlier_2_link_y = df_all_outlier_2_link_y

outlier_3_bytes_x = df_all_outlier_3_bytes_x
outlier_3_link_y = df_all_outlier_3_link_y

inlier_bytes_x = df_all_inlier_bytes_x
inlier_link_y = df_all_inlier_link_y

plt.figure(figsize = (10,5))
plt.title("one week")
plt.xlabel('G Bytes')
plt.ylabel('Total link')

red_point = plt.scatter(outlier_1_bytes_x, outlier_1_link_y, c='red', alpha=0.6, label='outlier')
green_point = plt.scatter(outlier_2_bytes_x, outlier_2_link_y, c='green', alpha=0.6, label='outlier')
black_point = plt.scatter(outlier_3_bytes_x, outlier_3_link_y, c='black', alpha=0.6, label='outlier')
blue_point = plt.scatter(inlier_bytes_x, inlier_link_y, c='blue', alpha=0.6, label='outlier')

#plt.legend([red_point, green_point, black_point, blue_point], ['anomaly flow -1, anomaly link -1',  'anomaly flow -1, anomaly link 1', 'anomaly flow 1, anomaly link -1', 'anomaly flow 1, anomaly link 1'], loc = 'upper right')    
plt.savefig('C:\\Users\\jia-ming\\Desktop\\figure.png')
plt.show()
"""

##########other############
import pandas as pd

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

df_all = pd.read_csv("10_11_65_169_two_feature_all.csv",encoding = 'big5')
#print(df_all)
#print()

total_bytes = df_all["total_bytes"]
total_link = df_all["total_link_count"]

import matplotlib.pyplot as plt
fig=plt.figure(figsize=(15,10))
plt.subplots_adjust(wspace =0, hspace =0.5)

plt.subplot(2,1,1)
plt.plot(total_bytes, label = 'G bytes')
plt.title("one week")
plt.xlabel('Time')
plt.ylabel('G bytes')
plt.xticks(rotation=90)  
plt.legend()

plt.subplot(2,1,2)
plt.plot(total_link, label = 'number of times')
plt.title("one week")
plt.xlabel('Time')
plt.ylabel('count')
plt.xticks(rotation=90)  
plt.legend()
plt.savefig('C:\\Users\\jia-ming\\Desktop\\figure1.png')
plt.show()


import matplotlib.pyplot as plt
fig=plt.figure(figsize=(10,5))
plt.subplots_adjust(wspace =0, hspace =0.5)

plt.subplot(2,1,1)
plt.style.use('ggplot')
plt.title("one week")
plt.xlabel('one minute flow')
plt.ylabel('number')
plt.hist(total_bytes, bins = 100, color = 'steelblue', edgecolor = 'k')

plt.subplot(2,1,2)
plt.style.use('ggplot')
plt.title("one week")
plt.xlabel('one minute count')
plt.ylabel('number')
plt.hist(total_link, bins = 100, color = 'steelblue', edgecolor = 'k')
plt.savefig('C:\\Users\\jia-ming\\Desktop\\figure2.png')
plt.show()


"""
import matplotlib.pyplot as plt
plt.style.use('ggplot')
plt.figure(figsize = (10,5))
plt.title("one week")
plt.xlabel('one minute flow')
plt.ylabel('number')
plt.hist(total_bytes, bins = 100, color = 'steelblue', edgecolor = 'k')
plt.show()
#plt.axis([0,1500000000,0,2000]) 


import matplotlib.pyplot as plt
plt.style.use('ggplot')
plt.figure(figsize = (10,5))
plt.title("one week")
plt.xlabel('one minute count')
plt.ylabel('number')
plt.hist(total_link, bins = 100, color = 'steelblue', edgecolor = 'k')
plt.show()
#plt.axis([0,1500000000,0,2000]) 
"""

