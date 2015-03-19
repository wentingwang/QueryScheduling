from QueryScheduler import Scheduler
import itertools
import sys
import copy

class OptimunScheduler(Scheduler):

    def __init__(self, NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, SORTER):
        Scheduler.__init__(self, NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, SORTER)

    def optimunSchedule(self):
        options = list(itertools.permutations(self.query_gen.taskList))
        print "permutation of query list size = ", len(options)
        min_metric = sys.maxint
        for query_list in options:
            metric = self.schedule(query_list)
            #print min_metric, metric
            if min_metric > metric:
                 min_metric = metric
                 min_query_list = copy.deepcopy(query_list)
            
            self.reset_node_queue()

        for query in min_query_list:
             print query.show() 
        return min_metric
    
    def reset_node_queue(self):
        while not self.node_queue.empty(): 
            node = self.node_queue.get() 
            node.currentTime = 0
            node. avail_slot = node.numSlot
            node.memory = []
        for query in self.query_gen.taskList:
            query.completionTime = 0
        map(self.node_queue.put, self.nodeGen.nodeList)
