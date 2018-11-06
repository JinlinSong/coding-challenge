from datetime import datetime
from collections import defaultdict
import json
import os
epoch = datetime.utcfromtimestamp(0)
payments_window = 60

class transact:
    def __init__(self, json):
        self.value = True
        try:
            self.created_time_sec = int((datetime.strptime(json['created_time'], '%Y-%m-%dT%H:%M:%SZ')-epoch).total_seconds())
            self.target = json['target']
            self.actor = json['actor']
        except:
            print("Error: Json format is invalid.")
            self.value = False
        if self.value:
            if not self.actor or not self.target:
                print("Error: Actor or target is null.")
                self.value = False
            elif self.actor == self.target:
                print("Error: Actor is the same with target.")
                self.value = False
    def is_valid(self):
        return self.value

class Graph:
    def __init__(self):
        self.graph = defaultdict(lambda: defaultdict(int))
        self.graph_degree = defaultdict(int)

    def add_transact(self, transact):
        if transact.created_time_sec > self.graph[transact.actor][transact.target]:
            if self.graph[transact.actor][transact.target] == 0:
                self.graph_degree[transact.actor] += 1.0
                self.graph_degree[transact.target] += 1.0
            self.graph[transact.actor][transact.target] = transact.created_time_sec
            self.graph[transact.target][transact.actor] = transact.created_time_sec
            return True
        else:
            return False
    
#    def del_payment(self, actor, target):
#        if actor in self.graph and target in self.graph[actor] and target in self.graph and actor in self.graph[target]:
#            del self.graph[actor][target]
#            del self.graph[target][actor]
#                
#            self.graph_degree[actor] -= 1.0
#            self.graph_degree[target] -= 1.0
#            return True
            
    def del_payment(self, actor, target):
        try:
            del self.graph[actor][target]
            del self.graph[target][actor]
                
            self.graph_degree[actor] -= 1.0
            self.graph_degree[target] -= 1.0
            return True
        except:
            return False

def median_degree(Graph):
    degree_list = list(Graph.graph_degree.values())
    degree_list = sorted(list(filter(lambda e: e != 0, degree_list)))
    if not degree_list:
        return None
    len_list = len(degree_list)
    middle_pos = len_list//2
    if len_list%2 == 0:
        return float((degree_list[middle_pos - 1] + degree_list[middle_pos])/2.0)
    else:
        return float(degree_list[middle_pos])

class process(object):
    def __init__(self):
        self.latest_transact = 0
        self.median_value = 0
        self.g = Graph() 
    def process_transact(self, transact):
        if transact.created_time_sec <= (self.latest_transact - payments_window):
            return self.median_value
        if transact.created_time_sec > self.latest_transact:
            self.latest_transact = transact.created_time_sec
            for i in self.g.graph:
                for j in list(self.g.graph[i]):
                    if self.g.graph[i][j] <= (self.latest_transact - payments_window) and i < j:
                        self.g.del_payment(i, j)
        self.g.add_transact(transact)
        self.median_value = median_degree(self.g)
        return self.median_value

def read_json(file_path):
    data = []
    with open(file_path) as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except:
                print("Error: Not able to load json.")
    return data

def write_median(file_path, median):
    with open(file_path, 'a') as f:
       f.write("{:.2f}".format(median) + '\n')
    f.close()

###############################      ###############################
############################### main ###############################
###############################      ###############################
input_path = r"C:\Users\jinlin.song\Desktop\Jinlin\coding-challenge\venmo_input\input_test.txt"
input_path1 = r"C:\Users\jinlin.song\Desktop\Jinlin\coding-challenge\data-gen\venmo-trans_abd.txt"
input_path2 = r"C:\Users\jinlin.song\Desktop\Jinlin\coding-challenge\data-gen\venmo-trans.txt"

output_path = r"C:\Users\jinlin.song\Desktop\Jinlin\coding-challenge\venmo_output\output_test.txt"

try:
    os.remove(output_path)
except OSError:
    pass

data1 = read_json(input_path1)
data2 = read_json(input_path2)

process_ = process()
for json_ in data:
    transact_point = transact(json_)
    if transact_point.is_valid():
        median_point = process_.process_transact(transact_point)
        write_median(output_path, median_point)
    else:
        pass

process_.g.graph_degree
    
transact_point.created_time_sec
process_.latest_transact
    

    


