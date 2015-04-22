from Query import Query, QueryGenerator, Comparitor
from Node import NodeGenerator,Node
from Placement import Placement
from MaxMatching import bipartiteMatch
import copy
import Queue
import math
import DataBlock
import random

class MatchingPlacement(Placement):
    #def __init__(self):
     #   Placement.__init__(self)

    def assign(self, node_list, block_list):
         #random assignment
         bipartite = {}
         alreadyLoad_block_list = set()
         for node in node_list:
             mapping=[]
             for block in block_list:
                 if block in node.memory:
                     mapping.append(block)
                     alreadyLoad_block_list.add(block)
             if len(mapping) != 0:
                 bipartite[node]=mapping
         '''
         print "bipartite graph:"
         for k,v in bipartite.items():
             print k.id,":[",
             for b in v:
                 print b.id,
             print "]" 
         ''' 
         num_busy_node = len(bipartite)
         num_idle_node = len(node_list) - num_busy_node

         num_alreadyLoad_block = len(alreadyLoad_block_list)
         num_needLoad_block = len(block_list) -  num_alreadyLoad_block

         num_block_per_node = int(math.ceil(len(block_list) * 1.0 / len(node_list)))
         if num_busy_node != 0:
             print "bipartite matching"
             num_slot = int (min(num_block_per_node, math.ceil(num_alreadyLoad_block * 1.0 / num_busy_node)))

	     '''
	     print "busy node:", num_busy_node, ", idle nodes:", num_idle_node
	     print "alread loaded block:", num_alreadyLoad_block, ",need load blocks:",  num_needLoad_block
	     print "num slots:", num_slot
	     '''
	     graph = {}
	     #replicate node to node * num_slot
	     for k,v in bipartite.items():
	         for i in range(num_slot):
	             node = copy.copy(k)
	             node.dup = random.randint(1, 10*num_slot)
	             graph[node]=v  
	     ''' 
	     print "temp graph"
	     for k,v in graph.items():
	         print k.id,":[",
	         for b in v:
	             print b.id,
	         print "]" 
	     '''
	     #find max matching
	     result = bipartiteMatch(graph)
	     maxmatching = result[0]
         else:
             maxmatching = {}
	 '''
	 print "max matching result:"
	 for k,v in maxmatching.items():
	     print k.id,":[",
	     print v.id,
	     print "]" 
	 '''
	 #return placement
	 placement = {}
	 assigned_block_list=set()
	 
	 for k,v in maxmatching.items():
	     node_id = v.id
	     for node in node_list:
                 if node.id == node_id:
                     thisNode = node
                     break
	     if thisNode in placement:
	         placement[node].append(k)
	     else:
	         placement[node]=[k]
	     assigned_block_list.add(k)
         '''
         print "After transformation:"
	 for k,v in placement.items():
	     print k.id,":[",
	     for block in v:
	         print block.id, " ",
	     print "]"
	 '''
	 #rebalance
	 #check if there is any busy node left idle
	 #TODO rebalance all busy nodes
	 if len(placement) != num_busy_node:
	     #find the left over idle node
	     need_block_node = []
	     for node in bipartite:
	         if node not in placement:
	             need_block_node.append(node)
	     for node in need_block_node:
	         for k,v in placement.items():
	             if len(v) > 1:
	                 for block in v:
	                     if block in node.memory:
	                          placement[node]=[block]
	                          v.remove(block)
	 '''        
	 print "After rebalance"
	 for k,v in placement.items():
	     print k.id,":[",
	     for block in v:
	         print block.id, " ",
	     print "]"
	  '''     
	 #deal with remaining blocks
	 for node in node_list:
	     if node not in bipartite:
	         placement[node]=[]

	 needLoad_block_list = list(set(block_list)-assigned_block_list)
	 '''
	 print "remaining blocks", len(needLoad_block_list)
	 for block in assigned_block_list:
	     print block.id," ",
	 '''
	 for k,v in placement.items():
	     while len(v) < num_block_per_node:
	             if needLoad_block_list:
	                 block = needLoad_block_list[0]
	                 placement[k].append(block)
	                 needLoad_block_list.remove(block)
	             else:
	                 break
	 '''                 
	 print "After dealing with remaining blocks"
	 for k,v in placement.items():
	     print k.id,":[",
	     for block in v:
	         print block.id, " ",
	     print "]"
         '''
         return placement
'''
#Test
nodeGen = NodeGenerator(5,1)
nodeGen.generate()
nodeGen.printAll()

node1 = nodeGen.nodeList[0]
node3 = nodeGen.nodeList[2]
node4 = nodeGen.nodeList[3]
node2 = nodeGen.nodeList[1]
Datagen = DataBlock.DataBlockGenerator(20,1)
Datagen.generate()
Datagen.printAll()

completionTime = node1.execute(Datagen.dataList[0:10])
node1.show()

completionTime = node2.execute(Datagen.dataList[0:3])
node2.show()

completionTime = node3.execute(Datagen.dataList[0:2])
node3.show()

completionTime = node4.execute(Datagen.dataList[0:2])
node4.show()

placement = MatchingPlacement()
placement.assign(nodeGen.nodeList, Datagen.dataList[0:20])
'''
