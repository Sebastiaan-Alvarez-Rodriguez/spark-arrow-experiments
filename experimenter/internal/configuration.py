
class CephConfiguration(object):
    '''Configuration describing what the Ceph cluster should look like, by specifying designations'''
    
    def __init__(self, designations):
        '''Initializes a Configuration object.
        Args:
            designations (list(list(rados_deploy.Designation))): List of designations that some unspecified node should have.
                                                           The mapping of configurations onto specific nodes is not handled by this system.'''
        if any(x for x in designations if not x):
            print('Designations contains empty designation-list: {}'.format(designations))
        self.designations = designations


    @property
    def num_osds(self):
        return self._num_osds


    def __len__(self):
        return len(self.designations)