import json
import os
from datetime import datetime
from collections import defaultdict
import sys
epoch = datetime.utcfromtimestamp(0)

def read_json(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except:
                print("Error: Not able to load json.")
    return data

path = r"C:\Users\jinlin.song\Desktop\Jinlin\coding-challenge\insight_testsuite\tests\mytest_1\venmo_input\venmo-trans.txt"
data = read_json(path)

class transact:
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
            if not self.actor or not self.target:
                print("Error: Actor or target is null.")
                self.value = False
            elif self.actor == self.target:
                print("Error: Actor is the same with target.")
                self.value = False
    def is_valid(self):
        return self.value

for e in data:
    print(transact(e).is_valid())

class Graph:
    ### Initialize Graph
    def __init__(self):
        self.graph = defaultdict(lambda: defaultdict(int))
        self.graph_degree = defaultdict(int)
        
        
    ### add edge
    def add_transact(self, transact):
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
    
    ### remove edge            
    def del_payment(self, actor, target):
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

g = Graph()

for record in data:
    g.add_transact(transact(record))

g.graph_degree
g.graph

### calculate median_degree
def median_degree(Graph):
    degree_list = list(Graph.graph_degree.values())
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

median_degree(Graph())
median_degree(g)

def write_median(file_path, value):
    with open(file_path, 'a') as f:
       f.write("{:.2f}".format(value) + '\n')
    f.close()

def process_data_stream():
    payments_window = 60
    latest_transact = 0
    median_value = None
    g = Graph()
    
    if len(sys.argv) != 2:
        print('Please specify two parameters: input path and output path')
        return False
    else:    
        input_path = sys.argv[1]
        output_f = open(sys.argv[2], 'w')
    
    json_dataset = read_json(input_path)
    
    for json_record in json_dataset:
        transact_record = transact(json_record)    
        if transact_record.created_time_sec <= (latest_transact - payments_window):
            output_f.write("{:.2f}".format(median_value) + '\n')
            return True
        else:
            if transact_record.created_time_sec > latest_transact:
                latest_transact = transact_record.created_time_sec
                for i in g.graph:
                    for j in list(g.graph[i]):
                        if g.graph[i][j] <= (latest_transact - payments_window) and i < j:
                            g.del_payment(i, j)
            g.add_transact(transact_record)
            median_value = median_degree(g)
            output_f.write("{:.2f}".format(median_value) + '\n')
    return True


    
        





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

def process():
    '''
    Method for processing payment records with moving window of 1 minute.
    simulates real time streaming by reading records one by one using python
    generator object to read lines from file.
    '''
    ### Initialize Graph
    g = Graph()
    ### Initialize activeNodes at current time
    activeRecords = []
    ### Initialize max_ts older date..
    max_ts = dateutil.parser.parse('1989-07-10T13:20:02Z')
    ### Process payment records....
    if len(sys.argv) == 0:
        print('Specify input and output arguments')    
    else:    
        f = open(sys.argv[1], 'r')
        wf = open(sys.argv[2], 'w')
        
    for data in f:
                payment = json.loads(data)
                ts = dateutil.parser.parse(payment['created_time'])
                ### Keep track of current max processing record
                if ts > max_ts:
                    max_ts = ts
                ### Update Graph based on latest payment record
                if ts >= (max_ts - timedelta(seconds=60)):
                    activeRecords.append((payment['actor'],payment['target'],ts))
                    ### Remove edges based on expiry of 1 minute windows
                    edgesToRemove = invalidateEdges(activeRecords, max_ts)
                    if len(edgesToRemove) >= 1:
                        g.removeEdges(edgesToRemove)
                        #### Update activeRecord list by removing the edges...
                        activeRecords = \
                           [record for record in activeRecords \
                              if (record[0],record[1]) not in edgesToRemove]
                    ### Update latest payment
                    g.addVertex(payment['actor'])
                    g.addEdge(payment['actor'], payment['target'])
                    writeOutput(median(g.getVerticesDegrees()), wf)
                else:
                    ### Write out median even in cases of out of order record
                    writeOutput(median(g.getVerticesDegrees()), wf)

if __name__ == '__main__':
    process()
    

if __name__ == '__main__':
    process()
    


