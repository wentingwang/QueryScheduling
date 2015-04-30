import DataBlock
import random

class Node(object):
    def __init__(self, id, numSlot):
        self.id = id
        self.numSlot = numSlot
        self.memory = []
        self.currentTime = 0
        self.avail_slot = numSlot
        self.load_data_time = 3
        self.dup = 0

    def show(self):
        print "id:",self.id, "currentTime:", self.currentTime, "avail_slot", self.avail_slot, "memory:", 
        for data in self.memory:
            print data.id,
        print 

    def execute(self, dataList):
        executionTime = 0;
        alreadLoad=[]
        needLoad=[]
        for data in dataList:
            if data in self.memory:
                alreadLoad.append(data)
            else:
                needLoad.append(data)
        #load and execute  
        #check loading time > computation time 
        for data in needLoad:
            executionTime += self.load_data_time
            #if computation time can fill load time
            if len(alreadLoad) > self.load_data_time:
                alreadLoad = alreadLoad[self.load_data_time:]
            #if computation time cannot fill load time
            else:
                alreadLoad=[]   
            #add new loaded data into memory
            self.memory.append(data)    
            alreadLoad.append(data)
        # see if there is any computation left over
        executionTime += len(alreadLoad)

        #print "execution time: ", executionTime

        completionTime = self.currentTime + executionTime
        self.currentTime = completionTime
        return (executionTime,completionTime)

    def __cmp__(self, other):
        if self.currentTime == other.currentTime:
            if self.avail_slot == other.avail_slot:
                 return cmp(self.id, other.id)
            else:
                 return cmp(self.avail_slot, other.avail_slot)
        else:
            return cmp(self.currentTime, other.currentTime)


class NodeGenerator(object):
    def __init__(self, numNode, median_numSlot):
        self.numNode = numNode;
        self.median_numSlot = median_numSlot
        self.nodeList = []
 
    def generate(self):
        for i in range( self.numNode):
            self.nodeList.append(Node(i,self.median_numSlot))

    def printAll(self):
        for node in self.nodeList:
            node.show()

'''
nodeGen = NodeGenerator(4,1)
nodeGen.generate()
nodeGen.printAll()

node1 = nodeGen.nodeList[0];
Datagen = DataBlock.DataBlockGenerator(10,1)
Datagen.generate()
Datagen.printAll()

completionTime = node1.execute(Datagen.dataList[0:7])
node1.show()

completionTime = node1.execute(Datagen.dataList)
node1.show()
'''
