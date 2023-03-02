# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 17:28:48 2018

@author: jia-ming
"""

import pandas as pd
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

df_all = pd.read_csv("169_11_08_two_feature.csv",encoding = 'big5')
print(df_all.head(20))
#print()

total_bytes = df_all["total_bytes"]
total_link = df_all["total_link_count"]

#####use log#######
#new_total_bytes = []
#new_total_link = []


#import math

#for i in total_bytes:
#    print(i)
#    new_total_bytes.append(math.log10(i))
   
#for i in new_total_link:
#    new_total_link.append(math.log10(i))
    
#total_bytes = new_total_bytes
#total_link = new_total_link
##################


import matplotlib.pyplot as plt
fig=plt.figure(figsize=(15,10))
plt.subplots_adjust(wspace =0, hspace =0.5)


plt.subplot(2,1,1)
plt.plot(total_bytes, label = 'bytes')
plt.title("flow")
plt.xlabel('Time')
plt.ylabel('bytes')
plt.xticks(rotation=90)  
plt.legend()


plt.subplot(2,1,2)
plt.plot(total_link, label = 'linkc count')
plt.title("link")
plt.xlabel('Time')
plt.ylabel('count')
plt.xticks(rotation=90)  
plt.legend()
plt.savefig('C:\\Users\\DUNCAN\\Desktop\\figure1.png')
#plt.savefig('C:\\Users\\jia-ming\\Desktop\\figure1.png')
plt.show()


import matplotlib.pyplot as plt
fig=plt.figure(figsize=(10,5))
plt.subplots_adjust(wspace =0, hspace =0.5)


plt.subplot(2,1,1)
plt.style.use('ggplot')
plt.title("flow")
plt.xlabel('log(one minute flow)')
plt.ylabel('frequency')
plt.hist(total_bytes, bins = 100, color = 'steelblue', edgecolor = 'k')

plt.subplot(2,1,2)
plt.style.use('ggplot')
plt.title("link")
plt.xlabel('one minute link')
plt.ylabel('frequency')
plt.hist(total_link, bins = 100, color = 'steelblue', edgecolor = 'k')
plt.savefig('C:\\Users\\DUNCAN\\Desktop\\figure2.png')
#plt.savefig('C:\\Users\\jia-ming\\Desktop\\figure2.png')
plt.show()



