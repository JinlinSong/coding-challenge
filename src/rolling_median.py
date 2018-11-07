# -*- coding: utf-8 -*-
"""
Created on Tue Nov  6 20:50:31 2018

@author: jinlin.song

Python version: Python 3.6.1
"""

import json
from datetime import datetime
from collections import defaultdict
import sys
epoch = datetime.utcfromtimestamp(0)

def read_json(file_path):
    '''
    Read all venmo payments from the given file. If the record is invalid, it will skip.
    '''
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except:
                print("Error: Not able to load json record, skip bad data.")
    f.close()
    return data

class transact:
    '''
    Convert venmo payment to python class transact.
    Each transact includes actor, target, and created_time_sec, the second differences from datetime.datetime(1970, 1, 1, 0, 0).

    Method is_valid will return False if the payment is not valid, such as missing data, actor is the same with target,
    and payment format is invalid.
    '''
    def __init__(self, json):
        self.value = True
        try:
            self.created_time_sec = int((datetime.strptime(json['created_time'], '%Y-%m-%dT%H:%M:%SZ') - epoch).total_seconds())
            self.target = json['target']
            self.actor = json['actor']
        except:
            print("Error: Json format is invalid.")
            self.value = False
        if self.value:
            ### mising actor or target
            if not self.actor or not self.target:
                print("Error: Actor or target is null.")
                self.value = False
            ### actor is the same with target
            elif self.actor == self.target:
                print("Error: Actor is the same with target.")
                self.value = False

    ### payment validation
    def is_valid(self):
        return self.value

class Graph:
    '''
    Define payment network as python class graph.
    For each payment, it only stores the lastest undirected payment created_time in graph, type defaultdict(defaultdict(int)).
    For each actor or target, it stores the number of total distinct connections in graph_degree, type defaultdict(int).

    Method add_transact to add new payment.
    Method del_payment to delete payment. 
    '''
    ### Initialize Graph
    def __init__(self):
        self.graph = defaultdict(lambda: defaultdict(int))
        self.graph_degree = defaultdict(int)
        
    ### method add_transact, add edge
    def add_transact(self, transact):
        '''
        Add new payment to payment network.
        If it is a new payment, add it to the graph, and update graph_degree.
        If it is a duplicated payment, with new created_time, update graph with the lastest created_time.
        If it is a old payment, skip.
        '''
        ### transact validation
        if transact.is_valid():
            ### created_time window validation
            if transact.created_time_sec > self.graph[transact.actor][transact.target]:
                ### duplicate edge validation
                if self.graph[transact.actor][transact.target] == 0:
                    self.graph_degree[transact.actor] += 1.0
                    self.graph_degree[transact.target] += 1.0
                ### update created_time
                self.graph[transact.actor][transact.target] = self.graph[transact.target][transact.actor] = transact.created_time_sec
                return True
            else:
                return False
        else:
            return False
    
    ### method del_payment, remove edge            
    def del_payment(self, actor, target):
        '''
        Remove undirected payment from graph. Delete record from graph, and update graph_degree. If graph_degree is 0, remove it.
        Since it is undirected, we remove edge, and update graph_degree from both sides.
        '''
        try:
            del self.graph[actor][target]
            if len(self.graph[actor]) == 0:
                del self.graph[actor]
                
            del self.graph[target][actor]
            if len(self.graph[target]) == 0:
                del self.graph[target]
                
            self.graph_degree[actor] -= 1.0
            if self.graph_degree[actor] == 0:
                del self.graph_degree[actor]
                
            self.graph_degree[target] -= 1.0
            if self.graph_degree[target] == 0:
                del self.graph_degree[target]
                
            return True
        except:
            return False

def median_degree(Graph):
    '''
    Calculate median_degree for given Graph, return float.
    '''
    degree_list = list(Graph.graph_degree.values())
    degree_list = list(filter(lambda e: e != 0, degree_list))

    if not degree_list:
        return None

    ### sort the Non-zero degree_list
    degree_list.sort()
    len_list = len(degree_list)
    middle_pos = len_list//2

    if len_list%2 == 0:
        return float((degree_list[middle_pos - 1] + degree_list[middle_pos])/2.0)
    else:
        return float(degree_list[middle_pos])

class process_data_stream:
    '''
    Class for processing venmo transacts.
    payments_window is 60 seconds.
    Process transact_record one by one, writing median value to given output file.
    '''
    def __init__(self, input_path,output_path):
        ### Default parameters, payments_window
        self.payments_window = 60
        
        ### Initialize parameters
        self.latest_transact = 0
        self.median_value = None
        
        ### Initialize Graph
        self.g = Graph()

        self.input_path = input_path
        self.output_f = open(output_path, 'w')

        ### load all venmo payments to json dataset
        self.json_dataset = read_json(input_path)


    def process_data(self, transact_record):
        '''
        Method to process one transact_record.
        '''
        if transact_record.is_valid():
            if transact_record.created_time_sec <= (self.latest_transact - self.payments_window):
                ### If the transact_record is out of payments_window, output the previous median value 
                self.output_f.write("{:.2f}".format(self.median_value) + '\n')
            else:
                ### Remove edges beyond payments_window
                if transact_record.created_time_sec > self.latest_transact:
                    self.latest_transact = transact_record.created_time_sec
                    for i in list(self.g.graph):
                        for j in list(self.g.graph[i]):
                            ### Remove payments which are prior to payments_window.
                            ### Since it is undirected, when we delete graph edge graph[i][j], and it will remove graph[j][i] at the same time   
                            if self.g.graph[i][j] <= (self.latest_transact - self.payments_window) and i < j:
                                self.g.del_payment(i, j)

                ### add latest payment
                self.g.add_transact(transact_record)

                ### update median
                self.median_value = median_degree(self.g)

                ### output median
                self.output_f.write("{:.2f}".format(self.median_value) + '\n')
        else:
            pass

    def close_file(self):
        self.output_f.close()

def main():
    ### get input path and out_put path
    if len(sys.argv) != 3:
        print('Please specify three parameters: rolling_median.py, input path and output path!')
        return False
    else:    
        input_path = sys.argv[1]
        output_path = sys.argv[2]

    process_data_stream_var = process_data_stream(input_path, output_path)

    ### Process venmo payment in json dataset
    for json_record in process_data_stream_var.json_dataset:
        transact_record = transact(json_record)
        process_data_stream_var.process_data(transact_record)

    process_data_stream_var.close_file()
    return True

if __name__ == '__main__':
    main()

