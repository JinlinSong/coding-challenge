# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 14:40:06 2018

@author: jinlin.song
"""
test = defaultdict(int)

### remove edge            
def del_payment(actor, target):
    try:
#        del test[actor][target]
#        if len(test[actor]) == 0:
#            del test[actor]
#            
#        del test[target][actor]
#        if len(test[target]) == 0:
#            del test[target]
            
        test[actor] -= 1.0
        if test[actor] == 0:
            del test[actor]
            
        test[target] -= 1.0
        if test[target] == 0:
            del test[target]
        return True
    except:
        return False
    
### calculate median_degree
def median_degree(degree_list):
    degree_list = list(filter(lambda e: e != 0, degree_list))
    if not degree_list:
        return None
    degree_list.sort()
    len_list = len(degree_list)
    middle_pos = len_list//2
    if len_list%2 == 0:
        return float((degree_list[middle_pos - 1] + degree_list[middle_pos])/2.0)
    else:
        return float(degree_list[middle_pos])

median_degree([3,1,2,4])