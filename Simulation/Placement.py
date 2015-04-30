from Query import Query, QueryGenerator, Comparitor
import DataBlock
from Node import NodeGenerator,Node
import Queue
import math


class Placement(object):
     
     def assign(self, node_list, block_list):
         for node in node_list:
             node.show()

         #random assignment
         placement = {}
         num_block_per_node = int(math.ceil(len(block_list) * 1.0 / len(node_list)))
         #print num_block_per_node 
         i = 0
         for node in node_list:
             placement[node] = block_list[i:i+num_block_per_node]
             i+=num_block_per_node

         print "Random--------"
	 total_inMemory = 0

	 for k,v in placement.items():
             inMemory_count = 0
	     print "id:",k.id,":[",
	     for block in v:
                 if block in k.memory:
	             print block.id, "* ",
                     inMemory_count+=1
                 else:
                     print block.id, " ",
	     print "] In Memory Count=", inMemory_count, "Not in Memory Count=", len(v)-inMemory_count
             total_inMemory += inMemory_count
         print "for this query: total in Memory:" ,total_inMemory
         
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
