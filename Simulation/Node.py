import DataBlock
import random

class Node(object):
    def __init__(self, id, numSlot):
        self.id = id
        self.numSlot = numSlot
        self.memory = []
        self.currentTime = 0
        self.avail_slot = numSlot

    def show(self):
        print "id:",self.id, "currentTime:", self.currentTime, "avail_slot", self.avail_slot, "memory:", 
        for data in self.memory:
            print data.id,
        print 

    def execute(self, dataList):
        executionTime = 0;
        for data in dataList:
#TODO deal with load time
            '''
            if data in self.memory:
                executionTime += data.computationTime
            else:
                self.memory.append(data)
                executionTime = executionTime + data.computationTime + 1
            '''
            executionTime += data.computationTime
        #print executionTime
        if self.avail_slot != self.numSlot:
            executionTime -= self.avail_slot
           
        d = executionTime  / self.numSlot
        r = executionTime  % self.numSlot 

        if r == 0:
            completionTime = self.currentTime + d
        else:
            completionTime = self.currentTime + d + 1

        self.currentTime = completionTime
        self.avail_slot = self.numSlot - r
        return completionTime

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
        for i in range(self.numNode):
            self.nodeList.append(Node(i,self.median_numSlot))

    def printAll(self):
        for node in self.nodeList:
            node.show()

'''
nodeGen = NodeGenerator(4,3)
nodeGen.generate()
nodeGen.printAll()

node1 = nodeGen.nodeList[0];
Datagen = DataBlock.DataBlockGenerator(10,1)
Datagen.generate()
completionTime = node1.execute(Datagen.dataList)
print completionTime
node1.show()
completionTime = node1.execute(Datagen.dataList[0:7])
print completionTime
node1.show()
'''

