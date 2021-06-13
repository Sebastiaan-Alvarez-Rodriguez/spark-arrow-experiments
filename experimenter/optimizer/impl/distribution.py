from experimenter.optimizer.optimizer import Optimization

class DistributionOptimization(Optimization):
    '''Simple class optimizing configurations based on node distribution.'''

    def __init__(self, keys=None):
        super(DistributionOptimization, self).__init__()
        self._keys = keys

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def keys(self):
        return self._keys

    @keys.setter
    def keys(self, value):
        self._keys = value


    def optimize(self, indexed_interfaces):
        '''Optimizes based on distribution keying.
        Returns:
            `list(list(int), list(int)...)`: Formed groups for optimization, with only global keys inside.'''
        if self._keys == None: # If no keys given, optimizes on all possible keys.
            self._keys = set(k for k,v in x.distribution.keys() for _,x in indexed_interfaces)

        groups = [list(indexed_interfaces)]
        for key in self._keys:
            newgroups = []
            for group in groups:
                if len(group) < 2: # We don't optimize a group of 1 member, as it cannot be altered anyway.
                    newgroups.append(group)
                    continue
                m = dict() # All interface that indicate possible optimization are grouped by the distribution length
                for x in group:
                    m.setdefault(len(x[1].distribution[key]) if x[1].distribution.optimize else 'no_optimization', []).append(x)
                for v in m.values():
                    newgroups.append(list(v))
            groups = newgroups
        return [[x[0] for x in group] for x in groups]


    def __eq__(self, other):
        if not isinstance(other, DistributionOptimization):
            return False
        return self.keys == other.keys


    def __hash__(self):
        return hash(self.keys)
