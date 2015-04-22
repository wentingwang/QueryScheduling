from Query import Query, QueryGenerator, Comparitor
import DataBlock
from Node import NodeGenerator,Node
import Queue
import math


class Placement(object):
     
     def assign(self, node_list, block_list):
         #random assignment
         placement = {}
         num_block_per_node = int(math.ceil(len(block_list) * 1.0 / len(node_list)))
         #print num_block_per_node 
         i = 0
         for node in node_list:
             placement[node] = block_list[i:i+num_block_per_node]
             i+=num_block_per_node
         
         return placement
'''
#test
nodeGen = NodeGenerator(4,1)
nodeGen.generate()
nodeGen.printAll()

Datagen = DataBlock.DataBlockGenerator(13,1)
Datagen.generate()
Datagen.printAll()

placement = Placement()
result = placement.assign(nodeGen.nodeList,Datagen.dataList)

for k,v in result.items():
   print k.id, " :",
   for block in v:
       print block.id, ",",
   print
'''
