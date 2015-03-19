import DataBlock
import random

class Comparitor:
    SIZE, RECENCY, PRIORITY, RANDOM = range(4)


class Query(object):
    def __init__(self, id, dataList, startTime, endTime, size):
        self.id = id
        self.dataList = dataList
        self.startTime = startTime
        self.endTime = endTime
        self.size = size
        self.completionTime = 0
    
    def show(self):
        return str(self.id) + ': '+ str(self.startTime)+'->'+str(self.endTime)+'\tsize:'+ str(self.size)+' priority:'+ str(self.size*self.startTime) + '\tcompletionTime:'+ str(self.completionTime)
        #for data in self.dataList:
        #    data.show()
        #print ""

class QueryGenerator(object):
    def __init__(self, dataBlockPool, totalNum, sizeDistribution, recencyDistribution):
         self.dataBlockPool = dataBlockPool
         self.totalNum = totalNum
         self.sizeDistribution = sizeDistribution
         self.recencyDistribution = recencyDistribution
         self.taskList = []

    def sort(self, comparitor):
        if comparitor == Comparitor.SIZE:
            self.taskList.sort(key=lambda query: query.size)
        elif comparitor == Comparitor.RECENCY:
            self.taskList.sort( key=lambda query: query.startTime)
        elif comparitor == Comparitor.PRIORITY:
            self.taskList.sort( cmp= priority_compare)
        elif comparitor == Comparitor.RANDOM:
            random.shuffle(self.taskList)
        else:
            return

    def generate(self, sortComparitor):
        #now only support random distribution(startTime first and size then)
        #TODO: implementation other distribution  
        random.seed(1)
        for i in range(self.totalNum):
            #print random.expovariate(1.0 / medianTaskSize);
            startTime = random.randrange(len(self.dataBlockPool))
            if startTime <= 0:
                i = i - 1
                continue
            endTime = startTime - random.randrange(startTime)
            self.taskList.append(Query(i,self.dataBlockPool[endTime:startTime],startTime,endTime,startTime-endTime))
        self.sort(sortComparitor)

    def printAll(self):
         for query in self.taskList:
             print query.show()



def priority_compare(q1,q2):
    return q1.startTime * q1.size - q2.startTime * q2.size
'''
Datagen = DataBlock.DataBlockGenerator(100,1)
Datagen.generate()
gen = QueryGenerator(Datagen.dataList, 10,1,1)
comparitor = Comparitor.RECENCY
#gen.generate(4,comparitor)
#gen.printAll()
'''
