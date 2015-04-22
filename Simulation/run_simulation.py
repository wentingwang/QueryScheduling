from Scheduler import Scheduler
from SchedulerRandom import RandomScheduler
from Query import Comparitor
from Placement import Placement
from PlacementMaxMatching import MatchingPlacement

NUM_NODE = 20
NUM_SLOT = 1
NUM_QUERY = 100
NUM_DATA = 1000
NUM_USED_NODE_PER_QUERY=20
SORTER = Comparitor.SIZE
SEED = 1
placement = Placement()
matchingPlacement = MatchingPlacement()


scheduler1 = RandomScheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA,NUM_USED_NODE_PER_QUERY, Comparitor.SIZE, placement)
metric = scheduler1.schedule()
scheduler1.printQueryList()
print "schedule: size\tplacement:random\t", metric

scheduler2 = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, NUM_USED_NODE_PER_QUERY,Comparitor.SIZE, matchingPlacement)
metric = scheduler2.schedule()
scheduler2.printQueryList()
print "schedule: size\tplacement:maxmatching\t", metric

'''
recency = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.RECENCY)
metric = recency.schedule()
recency.printQueryList()
print "schedule: recency\t", metric

priority = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.PRIORITY)
metric = priority.schedule()
priority.printQueryList()
print "schedule: priority\t", metric

random = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.RANDOM)
metric = random.schedule()
random.printQueryList()
print "schedule: random\t", metric

optimun = OptimunScheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.RANDOM)
metric = optimun.optimunSchedule()
print "schedule: optimun\t", metric
'''
