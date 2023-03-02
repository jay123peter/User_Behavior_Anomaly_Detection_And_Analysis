
# -*- coding: utf-8 -*-
"""

Created on Mon Apr  1 00:29:36 2019

@author: jia-ming

"""

##############################################
######file server to usb burst detection######
##############################################


from os import walk
import pandas as pd
import burst_detection as bd
import numpy as np
from datetime import timedelta

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

extract_file_server_path = "./extract_file_server"
log_file_server_path = "./merge_file_server"
log_usb_path = "./merge_usb"

control_day = 1 #control printer log range#
control_hour = 1 #control file server log range#

#Jaccard similarity score#
#Security_A:    noraml 0< warning < 0.1 abnormal
#Security_B:    noraml 0< warning < 0.3 abnormal
#Security_C:    noraml 0.3< warning < 0.5 abnormal
#Security_D:    noraml 0.3< warning < 0.7 abnormal
#Security_NONE:    noraml 0.7< warning < 0.9 abnormal

control_Security_A = 0.1
control_Security_B = 0.3
control_Security_C_1 = 0.5
control_Security_C_2 = 0.3
control_Security_D_1= 0.7
control_Security_D_2= 0.3
control_Security_none_1 = 0.9
control_Security_none_2 = 0.7

all_df = pd.DataFrame(columns=['user_name','burst_label', 'burst_time_start', 'burst_time_end',	'jaccard_score', 'state', 'intersection_quantity', 'intersection_file_name'])

#########################function#########################


#match file server burst time (1hour) to usb log(1day)#
def match_burst(burst_name, burst_file_server_df, log_file_server_df, log_usb_df):
    
    list_time_start = []
    list_time_end = []
    list_jaccard_score = []
    list_intersection_file_name = []
    list_warning = []
    list_burst_name = []
    list_user_name = []
    list_intersection_quantity = []
    
    #only read and move event#
    log_file_server_df = log_file_server_df[(log_file_server_df['event']=='read') | (log_file_server_df['event']=='move')].copy()
    
    #file server burst time#
    start_end_file_server_time_day = []
    start_end_file_server_time_hour = []
    
    for i in burst_file_server_df['datetime']:
   
        #burst time add one day for usb log#
        start_i = i
        end_i = start_i + timedelta(days=control_day)
        start_end_day = (start_i,end_i)
        start_end_file_server_time_day.append(start_end_day)
        
        #burst time add one hour for file server#
        start_x = i
        end_x = start_x + timedelta(hours=control_hour)
        start_end_hour = (start_x,end_x)
        start_end_file_server_time_hour.append(start_end_hour)

    burst_file_server_df = burst_file_server_df.set_index("datetime")
    log_file_server_df['time'] = pd.to_datetime(log_file_server_df['time'])
    log_usb_df['UD_TIME'] = pd.to_datetime(log_usb_df['UD_TIME'])      
    
    
    for i,x in zip(start_end_file_server_time_day,start_end_file_server_time_hour):
        
        extract_datetime_fileserver_df = log_file_server_df[log_file_server_df["time"].between(x[0],x[1])].copy()
        extract_datetime_usb_df = log_usb_df[log_usb_df['UD_TIME'].between(i[0],i[1])].copy()
        
        tmp_fileserver_filename_list = extract_datetime_fileserver_df["src_filename"].tolist()
        tmp_usb_filename_list = extract_datetime_usb_df["UUIDfileNM"].tolist()
        
        #if usb log empty represent no any log#
        if not tmp_usb_filename_list:
            continue
        
        #return jaccard_similarity score and intersection file name set#
        score,intersection_file_name_set = jaccard_similarity(tmp_usb_filename_list,tmp_fileserver_filename_list)
        str_file_name = ','.join(intersection_file_name_set)
        quantity = len(intersection_file_name_set)

        if score == 0:
            list_intersection_file_name.append("none")
            list_intersection_quantity.append(0)
            
        else:
            list_intersection_file_name.append(str_file_name)
            list_intersection_quantity.append(quantity)
        
        list_time_start.append(x[0])#file server burst time start#
        list_time_end.append(x[1])#file server burst time end#
        list_jaccard_score.append(score)
        list_user_name.append(tmp_user_name)

    #Judgment score abnormality, warning, normal#
    if burst_name == 'Security_A_burst' :

        for i in list_jaccard_score:
            
            list_burst_name.append(burst_name)
            
            if i >= control_Security_A:
                list_warning.append("anomaly")

            elif i == 0:
                list_warning.append("normal")

            else:
                list_warning.append("warning")

                
    elif burst_name == 'Security_B_burst' :
        
        for i in list_jaccard_score:
        
            list_burst_name.append(burst_name)
            
            if i >= control_Security_B:
                list_warning.append("anomaly")

            elif i == 0:
                list_warning.append("normal")

            else:
                list_warning.append("warning")

                
    elif burst_name == 'Security_C_burst' :
        
        for i in list_jaccard_score:
            
            list_burst_name.append(burst_name)
            
            if i >= control_Security_C_1:
                list_warning.append("anomaly")

            elif control_Security_C_2 < i < control_Security_C_1:
                list_warning.append("warning")

            else:
                list_warning.append("normal")

                
    elif burst_name == 'Security_D_burst' :
        
        for i in list_jaccard_score:
            
            list_burst_name.append(burst_name)
            
            if i >= control_Security_D_1:
                list_warning.append("anomaly")

            elif control_Security_D_2 < i < control_Security_D_1:
                list_warning.append("warning")

            else:
                list_warning.append("normal")
  
                
    elif burst_name == 'Security_none_burst' :
        for i in list_jaccard_score:
            
            list_burst_name.append(burst_name)
            
            if i >= control_Security_none_1:
                list_warning.append("anomaly")

            elif control_Security_none_2 < i < control_Security_none_1:
                list_warning.append("warning")

            else:
                list_warning.append("normal")
    
    #save to dataframe#
    df_user_name = pd.DataFrame(data = list_user_name, columns=['user_name'])
    df_burst_name = pd.DataFrame(data = list_burst_name, columns=['burst_label'])   
    df_file_server_burst_time = pd.DataFrame(data = list_time_start, columns=['burst_time_start'])
    df_usb_burst_time = pd.DataFrame(data = list_time_end, columns=['burst_time_end'])
    df_jaccard_score = pd.DataFrame(data = list_jaccard_score, columns=['jaccard_score'])
    df_intersection_file_name = pd.DataFrame(data = list_intersection_file_name, columns=['intersection_file_name'])
    df_intersection_quantity = pd.DataFrame(data = list_intersection_quantity, columns=['intersection_quantity'])
    df_warning = pd.DataFrame(data = list_warning, columns=['state'])
        
    frames = [df_user_name,df_burst_name,df_file_server_burst_time,df_usb_burst_time,df_jaccard_score,df_warning,df_intersection_quantity,df_intersection_file_name]

    df_result = pd.concat(frames,axis=1)
    
    return df_result

#burst detection function#
def burst(name, numpy_r, numpy_d, n, s, gamma, smooth_win, list_tmp_datetime):
    #d : total event, r : target event
    #p : a sequence of observed target probabilities
    #q : state sequence
    
    #find the optimal state sequence (q)
    q, d, r, p = bd.burst_detection(numpy_r, numpy_d, n, s, gamma, smooth_win)
      
    #enumerate bursts based on the optimal state sequence
    bursts_sequence = bd.enumerate_bursts(q, name, list_tmp_datetime)
     
    #find weight of bursts
    weighted_bursts = bd.burst_weights(bursts_sequence,r,d,p)
    
    #print("-----------------"+name+"-----------------")
    #print('observed probabilities: ')
    #print(str(r/d))
    #print()
    
    #print('optimal state sequence: ')
    #print(str(q.T))
    #print()
    
    #print('baseline probability: ' + str(p[0]))
    #print()
    
    #print('bursty probability: ' + str(p[1]))
    #print()
    
    #print('weighted bursts:')
    #print(weighted_bursts)
    #print()
    
    return q.T[0],weighted_bursts
    
#jaccard_similarity#
def jaccard_similarity(a, b):

    unions = len(set(a).union(set(b)))
    intersections = len(set(a).intersection(set(b)))
    intersection_file_name = set(a).intersection(set(b))
    
    if unions == 0:
        score = 0
    else:
        score = intersections / unions
    
    return  score, intersection_file_name

#########################main#########################
###############extract###################
extract_file_server_file_name = []

#file server#
for path, folder, files in walk(extract_file_server_path):
    for file_name in files:
        extract_file_server_file_name.append(file_name)
        
###############log###################
log_file_server_file_name = []
log_usb_file_name = []        

#file server#
for path, folder, files in walk(log_file_server_path):
    for file_name in files:
        log_file_server_file_name.append(file_name)

#usb#
for path, folder, files in walk(log_usb_path):
    for file_name in files:
        log_usb_file_name.append(file_name)

#usb log and file server log intersection file name#
log_set_file_server_file_name = set(log_file_server_file_name)
log_set_usb_file_name = set(log_usb_file_name)
log_list_file_name_intersection = list(log_set_file_server_file_name.intersection(log_set_usb_file_name))

print("log file server data total user:",len(log_set_file_server_file_name))
print("log usb data: total user:",len(log_set_usb_file_name))
print("log totol user:",len(log_list_file_name_intersection))
print()

#intersection file name and file server extract#
log_set_file_name_intersection = set(log_list_file_name_intersection)
extract_set_file_server_file_name = set(extract_file_server_file_name)
extract_list_file_name_intersection = list(extract_set_file_server_file_name.intersection(log_set_file_name_intersection))
print("extract file server data total user:",len(extract_file_server_file_name))
print("extract totol user:",len(extract_list_file_name_intersection))
print()

for extract_file_name, log_file_name in zip(extract_list_file_name_intersection,log_list_file_name_intersection): 
    print('extract:',extract_file_name)
    print('log:',log_file_name)
    print()

    #log read#
    log_file_server_all_path = log_file_server_path +'/'+log_file_name
    log_file_server_df = pd.read_csv(log_file_server_all_path, encoding = "utf-8-sig",low_memory=False)
    
    log_usb_all_path = log_usb_path +'/'+log_file_name
    log_usb_df = pd.read_csv(log_usb_all_path, encoding = "utf-8-sig",low_memory=False)
    
    #file server extract read#
    file_server_all_path = extract_file_server_path +'/'+ extract_file_name
    file_server_df = pd.read_csv(file_server_all_path, encoding = "utf-8",low_memory=False)
    
    ##############file server burst##############
    #d->sum#
    list_file_server_d = file_server_df['sum'].tolist()
    new_list_file_server_d = []
    for i in list_file_server_d:
        result = i + 1
        new_list_file_server_d.append(result)
    numpy_file_server_d = np.array(new_list_file_server_d, dtype=float)
    
    #r->security_A#
    list_file_server_Security_A_r = file_server_df['security_A'].tolist()
    numpy_file_server_Security_A_r = np.array(list_file_server_Security_A_r, dtype=float)
    file_server_Security_A_n = len(numpy_file_server_Security_A_r)
 
    #r->security_B#
    list_file_server_Security_B_r = file_server_df['security_B'].tolist()
    numpy_file_server_Security_B_r = np.array(list_file_server_Security_B_r, dtype=float)
    file_server_Security_B_n = len(numpy_file_server_Security_B_r)
    
    #r->security_C#
    list_file_server_Security_C_r = file_server_df['security_C'].tolist()
    numpy_file_server_Security_C_r = np.array(list_file_server_Security_C_r, dtype=float)
    file_server_Security_C_n = len(numpy_file_server_Security_C_r)
    
    #r->security_D#
    list_file_server_Security_D_r = file_server_df['security_D'].tolist()
    numpy_file_server_Security_D_r = np.array(list_file_server_Security_D_r, dtype=float)
    file_server_Security_D_n = len(numpy_file_server_Security_D_r)
    
    #r->security_none#
    list_file_server_Security_none_r = file_server_df['security_none'].tolist()
    numpy_file_server_Security_none_r = np.array(list_file_server_Security_none_r, dtype=float)
    file_server_Security_none_n = len(numpy_file_server_Security_none_r)
    
    file_server_df['datetime'] =  pd.to_datetime(file_server_df['datetime'])
    list_fileserver_datetime = file_server_df['datetime'].tolist()
    
    #####detection busrt#####
    file_server_Security_A_q,file_server_Security_A_weighted_bursts = burst('fileserver_Security_A', numpy_file_server_Security_A_r, numpy_file_server_d, file_server_Security_A_n, s=1.5, gamma=0.5, smooth_win=1, list_tmp_datetime = list_fileserver_datetime)
    file_server_df["Security_A_state"] = file_server_Security_A_q

    file_server_Security_B_q,file_server_Security_B_weighted_bursts = burst('fileserver_Security_B',numpy_file_server_Security_B_r,numpy_file_server_d,file_server_Security_B_n,s=1.5,gamma=0.1,smooth_win=1, list_tmp_datetime = list_fileserver_datetime)
    file_server_df["Security_B_state"] = file_server_Security_B_q
        
    file_server_Security_C_q,file_server_Security_C_weighted_bursts = burst('fileserver_Security_C',numpy_file_server_Security_C_r,numpy_file_server_d,file_server_Security_C_n,s=1.5,gamma=0.1,smooth_win=1, list_tmp_datetime = list_fileserver_datetime)
    file_server_df["Security_C_state"] = file_server_Security_C_q

    file_server_Security_D_q,file_server_Security_D_weighted_bursts = burst('fileserver_Security_D',numpy_file_server_Security_D_r,numpy_file_server_d,file_server_Security_D_n,s=1.5,gamma=0.1,smooth_win=1, list_tmp_datetime = list_fileserver_datetime)
    file_server_df["Security_D_state"] = file_server_Security_D_q
    
    file_server_Security_none_q,file_server_Security_none_weighted_bursts = burst('fileserver_Security_none',numpy_file_server_Security_none_r,numpy_file_server_d,file_server_Security_none_n,s=1.5,gamma=0.1,smooth_win=1, list_tmp_datetime = list_fileserver_datetime)
    file_server_df["Security_none_state"] = file_server_Security_none_q

    ##########################
    ###extract have burst data###
    #0:base line 1:burst 
    
    #file server brust#
    burst_file_server_Security_A_df = file_server_df[file_server_df['Security_A_state']==1].copy()
    burst_file_server_Security_B_df = file_server_df[file_server_df['Security_B_state']==1].copy()
    burst_file_server_Security_C_df = file_server_df[file_server_df['Security_C_state']==1].copy()
    burst_file_server_Security_D_df = file_server_df[file_server_df['Security_D_state']==1].copy()
    burst_file_server_Security_none_df = file_server_df[file_server_df['Security_none_state']==1].copy()
  
    ##########################
    ##to_datetime##
 
    #file server#
    burst_file_server_Security_A_df['datetime'] = pd.to_datetime(burst_file_server_Security_A_df['datetime'])
    burst_file_server_Security_B_df['datetime'] = pd.to_datetime(burst_file_server_Security_B_df['datetime'])
    burst_file_server_Security_C_df['datetime'] = pd.to_datetime(burst_file_server_Security_C_df['datetime'])
    burst_file_server_Security_D_df['datetime'] = pd.to_datetime(burst_file_server_Security_D_df['datetime'])
    burst_file_server_Security_none_df['datetime'] = pd.to_datetime(burst_file_server_Security_none_df['datetime'])
    
    ##########################
    ###save weighted bursts###

    tmp_all_weighted_bursts = pd.concat([file_server_Security_A_weighted_bursts,file_server_Security_B_weighted_bursts,file_server_Security_C_weighted_bursts,file_server_Security_D_weighted_bursts,file_server_Security_none_weighted_bursts], ignore_index=True)
    tmp_user_name = extract_file_name.split('.')[0]
    save_csv_name = tmp_user_name+"_all_weight_table.csv"
    tmp_all_weighted_bursts.to_csv(save_csv_name,index = False, encoding ='utf-8')

    ############## match log##############
    Security_A_df_match_burst = match_burst("Security_A_burst",burst_file_server_Security_A_df,log_file_server_df,log_usb_df)
    Security_B_df_match_burst = match_burst("Security_B_burst",burst_file_server_Security_B_df,log_file_server_df,log_usb_df)
    Security_C_df_match_burst = match_burst("Security_C_burst",burst_file_server_Security_C_df,log_file_server_df,log_usb_df)
    Security_D_df_match_burst = match_burst("Security_D_burst",burst_file_server_Security_D_df,log_file_server_df,log_usb_df)
    Security_none_df_match_burst = match_burst("Security_none_burst",burst_file_server_Security_none_df,log_file_server_df,log_usb_df)
    
    tmp_all_match_burst = pd.concat([Security_A_df_match_burst,Security_B_df_match_burst,Security_C_df_match_burst,Security_D_df_match_burst,Security_none_df_match_burst], ignore_index=True)
    
    ###save only one user data###
    save_csv_name = tmp_user_name+"_all_match_burst_table.csv"
    tmp_all_match_burst.to_csv(save_csv_name,index = True, encoding ='utf-8')

    ###jaccard score>0###
    tmp_df_merge = tmp_all_match_burst[tmp_all_match_burst['jaccard_score']>0]
    all_df = all_df.append(tmp_df_merge, ignore_index=True,  sort=False)

###save jaccard score>0###
save_csv_name = "anomaly_match_burst_table.csv"
all_df.to_csv(save_csv_name,index = True, encoding ='utf-8')
