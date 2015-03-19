from QueryScheduler import Scheduler
from Query import Comparitor
from OptimunScheduler import OptimunScheduler

NUM_NODE = 4
NUM_SLOT = 2
NUM_QUERY = 5
NUM_DATA = 1000
SORTER = Comparitor.SIZE
SEED = 1

random = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.RANDOM)
metric = random.schedule()
random.printQueryList()
print "schedule: random\t", metric

size = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.SIZE)
metric = size.schedule()
size.printQueryList()
print "schedule: size\t", metric


recency = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.RECENCY)
metric = recency.schedule()
recency.printQueryList()
print "schedule: recency\t", metric

priority = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.PRIORITY)
metric = priority.schedule()
priority.printQueryList()
print "schedule: priority\t", metric

optimun = OptimunScheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, Comparitor.RANDOM)
metric = optimun.optimunSchedule()
print "schedule: optimun\t", metric

