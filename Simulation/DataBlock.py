class DataBlock(object):
    def __init__(self, id, startTime, endTime, computationTime):
        self.id = id
        self.startTime = startTime
        self.endTime = endTime
        self.computationTime = computationTime
    def show(self):
        print "id: ", self.id , " " , self.startTime ,"->" , self.endTime , "computationNeed" , self.computationTime

class DataBlockGenerator(object):
    def __init__(self, totalNum, distribution):
        self.totalNum = totalNum
        self.distribution = distribution
        self.dataList = []

    def generate(self):
        for i in range(self.totalNum):
            self.dataList.append(DataBlock(i, i, i+1, 1))

    def printAll(self):
        for data in self.dataList:
            data.show()

#TEST
#gen = DataBlockGenerator(100,1)
#gen.generate()
#gen.printAll()
