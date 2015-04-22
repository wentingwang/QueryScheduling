from Scheduler import Scheduler
import itertools
import sys
import copy
import random

class RandomScheduler(Scheduler):
     def __init__(self, NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, NUM_USED_NODE, SORTER, placement):
        super(RandomScheduler, self).__init__(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, NUM_USED_NODE, SORTER, placement)
        self.node_queue = set(self.nodeGen.nodeList)
        
        

     def schedule(self, taskList=None):
        #maintain a priority queue based on available nodes(who has the smallest current time)
        #for each query
        #    do round robin
        totalCompletionTime = 0.0
        totalExecutionTime = 0.0
        if taskList is None:
           taskList = self.query_gen.taskList
        num_query = len(taskList)
        #Layer1 scheduler: schedule the order of queries
        #TODO reschedule the task list if needed

        #Layer2 scheduler: the placement/assignment of queries to nodes
        for query in taskList:
            if query.size != 0:
                
                node_list = []
                num_used_node = min(self.NUM_USED_NODE, self.NUM_NODE)
                #randomly choose some nodes
                node_list= random.sample(self.node_queue,num_used_node)
    		
                assignment = self.placement.assign(node_list,query.dataList)
                for k,v in assignment.items():
                    
                    (executionTime,completionTime) = k.execute(v)
                    print k.id, " :",
                    for block in v:
                        print block.id, ",",
                    print "completion time:", completionTime

                    query.completionTime = query.completionTime if query.completionTime > completionTime else completionTime
                    self.node_queue.add(k)
                print "Dealing with Query: ", query.show()
                totalCompletionTime += query.completionTime 
        return totalCompletionTime/num_query;
