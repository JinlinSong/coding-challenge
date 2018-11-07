import json
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

def process_data_stream():
    '''
    Function for processing transact data_stream. payments_window is 60 seconds.
    Process transact_record one by one, writing median_value to output file.
    '''
    
    ### Default parameters
    payments_window = 60
    
    ### Initialize parameters
    latest_transact = 0
    median_value = None
    
    ### Initialize Graph
    g = Graph()
    
    ### get input path and out_put path
    if len(sys.argv) != 2:
        print('Please specify two parameters: input path and output path')
        return False
    else:    
        input_path = sys.argv[1]
        output_f = open(sys.argv[2], 'w')
    json_dataset = read_json(input_path)
    
    ### Process transact_records json_dataset
    for json_record in json_dataset:
        transact_record = transact(json_record)
        if transact_record.is_valid():
            if transact_record.created_time_sec <= (latest_transact - payments_window):
                ### Output the previous median value if the transact_record is out of payments_window
                output_f.write("{:.2f}".format(median_value) + '\n')
            else:
                if transact_record.created_time_sec > latest_transact:
                    latest_transact = transact_record.created_time_sec
                    for i in g.graph:
                        for j in list(g.graph[i]):
                            if g.graph[i][j] <= (latest_transact - payments_window) and i < j:
                                ### Remove edges beyond payments_window
                                g.del_payment(i, j)
                ### add latest payment
                g.add_transact(transact_record)
                median_value = median_degree(g)
                output_f.write("{:.2f}".format(median_value) + '\n')
        else:
            pass
    output_f.close()
    return True

if __name__ == '__main__':
    process_data_stream()
    

