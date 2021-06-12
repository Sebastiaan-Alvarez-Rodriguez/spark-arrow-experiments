from utils.structures.priorityqueue import PriorityQueue
from itertools import chain

class Optimization(Enum):
    NODE_DISTRIBUTION = 0,
    DATA_DISTRIBUTION = 1


class Optimization(object):
    def __init__(self, interfaces):
        self._interfaces = interfaces

    def optimize(self):
        raise NotImplementedError('Did not implement optimize function for optimization "{}"'.format(self.__class__.__name__))


class DistributionOptimization(Optimization):
    def __init__(self, interfaces):
        super(ClassName, self).__init__(interfaces)

    def _comperator(self, x, y):
        dist_x = x.distribution
        dist_y = y.distribution
        return dist_x['spark'] > dist_y['spark']

    def optimize(self):
        if isinstance(self.interfaces, list):
            all_keys = set(chain(list(x.distribution.keys()) for x in self.interfaces))
            optimizable_keys = set() # TODO: Make something that defines whether a distribution group is optimizable
            from functools import cmp_to_key # Use this to be able to have a key=cmp_to_key(lambda x, y: x-y) instead of key=lambda e: (len(e), e[0])
            sort(self.interfaces, key=lambda e: [len(e.distribution[name]) for name in optimizable_keys])

        

class OptimizationConfig(object):
    def __init__(self, optimizations=[], optimizations_comperator=lambda x, y: x > y):
        self._optimizations = PriorityQueue(comperator=optimizations_comperator)
        self._optimizations.insert(optimizations)


    def optimization_add(self, val):
        '''Add a new priority, with given priority integer.
        Args:
            val (Any): Value to insert.'''
        self._optimizations.insert(val)


    def optimization_iterate(self):
        return self._optimizations.iterate()


def optimize(experiment_interfaces, optimize_config):
    
    for priority in optimize_config.optimization_iterate():
        pass