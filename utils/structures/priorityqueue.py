class PriorityQueue(object):
    def __init__(self, comperator=lambda x,y: x > y):
        self.q = []
        self.comperator = comperator


    def _binary_insert(low, high, item):
        mid = (low+high)//2
        cur_item = self.q[mid]
        if (low == high):
            self.q.insert(low, item)
            return low
        if (self.comperator(item, cur_item)):
            return binary_insert(low, (mid-1), item)
        else:
            return binary_insert(mid+1, high, item)


    def get(self, idx):
        return self.q.get(idx)


    def insert(self, *items):
        return [_binary_insert(0, len(self), x) for x in items]


    def iterate(self):
        return list(self.q)


    def pop_front():
        return self.q.pop(0)


    def pop_back():
        return self.q.pop()


    def __len__(self):
        return len(self.q)
