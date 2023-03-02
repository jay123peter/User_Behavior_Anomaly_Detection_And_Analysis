# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:19:02 2019

@author: DUNCAN
"""


import burst_detection as bd
import numpy as np

print(np.log(0))

"""
#number of target events at each time point
r = np.array([0, 2, 1, 6, 7, 2, 8, 7, 2, 1], dtype=float)
#total number of events at each time point
d = np.array([9, 11, 12, 10, 10, 8, 12, 10, 13, 11], dtype=float)
#number of time points
n = len(r)

#find the optimal state sequence (q)
q, d, r, p = bd.burst_detection(r,d,n,s=2,gamma=1,smooth_win=1)

#enumerate bursts based on the optimal state sequence
bursts = bd.enumerate_bursts(q, 'burstLabel')

#find weight of bursts
weighted_bursts = bd.burst_weights(bursts,r,d,p)

print('observed probabilities: ')
print(str(r/d))

print('optimal state sequence: ')
print(str(q.T))

print('baseline probability: ' + str(p[0]))

print('bursty probability: ' + str(p[1]))

print('weighted bursts:')
print(weighted_bursts)
"""