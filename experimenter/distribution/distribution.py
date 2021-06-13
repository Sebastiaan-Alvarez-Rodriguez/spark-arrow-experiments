

class Distribution(object):
    '''Class representing a distribution.
    Args:
        mapping (dict(str, Distribution.Element)): Map mapping single distribution names to an element. An element tells what nodes were reserved for this distribtuion, and whether we can optimize.'''
    def __init__(self, elements=None, mapping=None):
        if (elements and mapping) or (not elements and not mapping):
            raise ValueError('Require either "elements" or "mapping" to be set.')
        if elements:
            if any(True for x in elements if not isinstance(x, Element)):
                raise ValueError('Distribution mapping values must have "Element" type. Given keys have inccorect value type:\n{}'.format('\n'.join('    value (type={})={}'.format(type(x), x) for x in elements if not isinstance(x, Element))))
            self._mapping = {x.name: x for x in elements}
        else:
            if not isinstance(mapping, dict):
                raise ValueError('Distribution mapping must have "dict" type, found: {}'.format(type(mapping)))
            if any(mapping) and not isinstance(next(x for x in mapping), str):
                raise ValueError('Distribution mapping keys must have "str" type, found: {}'.format(type(next(x for x in mapping))))
            if any(mapping) and any(True for x in mapping if not isinstance(mapping[x], Element)):
                raise ValueError('Distribution mapping values must have "Element" type. Given keys have inccorect value type:\n{}'.format('\n'.join('    key={}, value (type={})={}'.format(x, type(mapping[x]), mapping[x]) for x in mapping if not isinstance(mapping[x], Element))))
            self._mapping = mapping


    def __getitem__(self, key):
        return self._mapping[key]

    def get(self, name):
        return self._mapping.get(name)

    def keys(self):
        return self._mapping.keys()

    def items(self):
        return ((x, self._mapping[x]) for x in self._mapping.keys())

    def values(self):
        return self._mapping.values()

    def add(self, name, elem):
        self._mapping[name] = elem

    def remove(self, name):
        del self._mapping[name]

    def __len__(self):
        return len(self._mapping)


    class Element(object):
        '''Class representation of a single element of a distribution. Contains a name, a set of nodes, and a flag indicating whether optimization in distribution is possible.'''
        def __init__(self, name, nodes, optimize=True):
            self._name = name
            self._nodes = nodes
            self._optimize = optimize

        @property
        def name(self):
            return self._name

        @property
        def nodes(self):
            return self._nodes

        @property
        def optimize(self):
            return self._optimize

        def contains(self, node):
            return node in self._nodes

        def __str__(self):
            return '{}: {}'.format(self._name, ', '.join(x.hostname for x in self._nodes))
        
        def __hash__(self):
            return hash(self._name)

        def __len__(self):
            return len(self._nodes)