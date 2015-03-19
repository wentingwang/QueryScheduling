from Query import Query, QueryGenerator, Comparitor
import DataBlock
from Node import NodeGenerator,Node
import Queue
import math



class Scheduler(object):
    def __init__(self, NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, SORTER):
        self.NUM_NODE = NUM_NODE
        self.NUM_SLOT = NUM_SLOT
        self.NUM_QUERY = NUM_QUERY
        self.NUM_DATA = NUM_DATA
        self.SORTER = SORTER
        self.setup()
    
    def setup(self):
        #Generate Data blocks
        self.data_gen = DataBlock.DataBlockGenerator(self.NUM_DATA,1)
        self.data_gen.generate()

        #Generate Nodes
        self.nodeGen = NodeGenerator(self.NUM_NODE, self.NUM_SLOT)
        self.nodeGen.generate()
        self.node_queue = Queue.PriorityQueue()
        #node_queue.put(nodeGen.nodeList)
        map(self.node_queue.put, self.nodeGen.nodeList)


        '''
        while not node_queue.empty(): 
            node = node_queue.get() 
            node.show()
        map(node_queue.put, nodeGen.nodeList)
        '''

        #Generate Query
        self.query_gen = QueryGenerator(self.data_gen.dataList, self.NUM_QUERY,1,1)
        self.query_gen.generate(self.SORTER)
        self.query_gen.printAll()

    def schedule(self, taskList=None):
        #maintain a priority queue based on available nodes(who has the smallest current time)
        #for each query
        #    do round robin

        if taskList is None:
           taskList = self.query_gen.taskList
        metric = 0.000000
        for query in taskList:
            if query.size != 0:
                num_node_used = math.ceil(query.size * 1.0 / self.NUM_SLOT)
                num_node_fullused = query.size / self.NUM_SLOT
                #print query.show(), 'I want ', num_node_used, 'node'
                for i in range(int(num_node_used)):
                    node = self.node_queue.get();
                    if i<num_node_fullused:
                        completionTime = node.execute(query.dataList[i*self.NUM_SLOT:(i+1)*self.NUM_SLOT])             
                    else:
                        completionTime = node.execute(query.dataList[i*self.NUM_SLOT:]) 

                    query.completionTime = query.completionTime if query.completionTime > completionTime else completionTime
                    self.node_queue.put(node)
                metric += query.completionTime * 1000000/ (query.startTime * query.size);

        #for query in taskList:
        #     print query.show()        
        return metric

    def printQueryList(self):
        
        for query in self.query_gen.taskList:
             print query.show()


        
        

