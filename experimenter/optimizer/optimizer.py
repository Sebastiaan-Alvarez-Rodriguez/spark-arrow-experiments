from utils.structures.priorityqueue import PriorityQueue
from itertools import chain
import inspect


class Optimization(object):
    def __init__(self):
        pass

    def optimize(self, indexed_interfaces):
        raise NotImplementedError('Did not implement optimize function for optimization "{}"'.format(self.__class__.__name__))


class OptimizationConfig(object):
    def __init__(self, optimizations=[], comperator=lambda x, y: x > y):
        self._comperator = comperator
        self._optimizations = PriorityQueue(comperator=comperator)
        self._optimizations.insert(*optimizations)

    @property
    def comperator(self):
        return self._comperator
    

    def optimization_add(self, val):
        '''Add a new optimization.
        Args:
            val (Any): Value to insert.'''
        self._optimizations.insert(val)


    def optimization_iterate(self):
        return self._optimizations.iterate()


    def __eq__(self, other):
        if not isinstance(other, OptimizationConfig):
            return False
        return self._optimizations == other._optimizations


    def __hash__(self):
        return hash(self._optimizations)


    def __len__(self):
        return len(self._optimizations)

'''
Think about this:
distr='spark':10, data=A
distr='spark':10, data=B
distr='spark':10, data=A
...
If we optimize this group on data, we get the most efficient data output.
In a matrix:
0, 10, A
1, 10, B
2, 08, A
3, 10, A

There is 1 group at start. We simply sort on column=1.
0, 10, A
1, 10, B
3, 10, A
2, 08, A

There are 2 groups now: col1=10, col1=8. We sort WITHIN a group for the other values:
0, 10, A
3, 10, A
1, 10, B
2, 08, A
Note: first using dataset A and then B or first B and then A does not matter, as the nodes are different anyways.
'''
def optimize(execution_interfaces, optimize_config):
    '''Apply an optimization config to a series of ExecutionInterfaces.
    Args:
        execution_interfaces (list(ExecutionInterface)): ExecutionInterfaces to apply optimizations on.
        optimize_config (OptimizationConfig): Config to optimize with.

    Returns:
        list(ExecutionInterface)): Reordered ExecutionInterfaces in such a way that optimizations between runs are possible.'''
    groups = [list(range(len(execution_interfaces)))]
    optimizations_checked = []
    for optimization in optimize_config.optimization_iterate():
        print('Optimizing for {}'.format(optimization.name))
        if inspect.isclass(optimization):
            optimization = optimization()
        if not isinstance(optimization, Optimization):
            raise ValueError('Non-Optimization class specified: {}'.format(type(optimization)))
        optimizations_checked.append(optimization)

    for optimization in optimizations_checked:
        newgroups = []
        for group in groups:
            if len(group) < 2: # We don't optimize a group of 1 member, as it cannot be altered anyway.
                newgroups.append(group)
                continue
            instance = optimization([(x, execution_interfaces[x]) for x in group]) # We optimize only within a group at a time
            # We want to have back in our example
            # iteration 0: [0,1,2,3] -> [[0,1,3], [2]]
            # iteration 1: [0,1,3] -> [0,3,1]. [2] -> [2]
            newgroups += instance.optimize()
        print('Optimization "{}" applied on all groups. Identified {} extra optimization group(s).'.format(optimization.name, len(groups)-len(newgroups)))
        groups = newgroups
    return [execution_interfaces[idx] for idx in groups]