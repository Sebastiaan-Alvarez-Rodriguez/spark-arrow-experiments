def _default_node_configuration():
    return NodeConfiguration(8, CephConfiguration(
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD]))


class ExperimentConfiguration(object):
    '''Data class containing all kinds of configurable parameters for experiments.'''
    def __init__(self):
        # Spark cluster options
        self.spark_silent = False
        self.spark_workdir = ''
        
        # RADOS-Ceph cluster options
        self.ceph_silent = False
        self.ceph_compile_cores = 16
        self.ceph_mountpoint_path = '/mnt/cephfs'

        #Shared cluster options
        self.silent = False # Overrides both spark_silent and ceph_silent if set to `True`.
        self.key_path = '~/.ssh/geni.rsa'

        # Data deployment params
        self.stripe = 64 # Generate a parquet file for a stripe-constraint of X MB.
        self.data_multiplier = 20 # makes dataset this factor larger using symlinks (default value multiplies to 64*20=1280MB).

        # Application deployment params
        self.jar = 'arrow-spark-benchmark-1.0-light.jar'
        self.mainclass = 'org.arrowspark.benchmark.Benchmark'
        self.args = '{} -np {} -r {} -p {}/ --format {} -nr {} -dm {} -cl {} -nc {} -cc {}'
        self.extra_jars = None
        self.offheap_memory = None #1024*1024*1 # 1 mb of off-heap memory per JVM. Set to None to disable offheap memory
        self.submit_opts = None
        self.shared_submit_opts = None

        self.no_results_dir = True
        self.eventlog_path = None  # Set this to an existing directory to make Spark history server logs
        self.flamegraph_time = None
        self.flamegraph_only_master = False
        self.flamegraph_only_worker = False
        self.resultloc = fs.join(fs.abspath(), '..', 'base_res')

        # Experiment params
        self.node_config = _default_node_configuration() # Must be a `NodeConfiguration`. Note: Number of Ceph-nodes must be at least 3.
        self.extensions = 'pq'
        self.compressions = 'uncompressed'
        self.compute_columns = 4
        self.kinds = 'df'
        self.rbs = 8192
        self.test_modes = '--arrow-only', '--spark-only'
        self.custom_rb_func = None #Function to override rbs for experiments with variable amounts of input data

        self.runs = 31 # We run our implementation and the Spark baseline implementation X times
        self.retries = 2 # If our application dies X times, we stop trying and move on
        self.appl_sleeptime = 30 # Sleep X seconds between checks
        self.appl_dead_after_tries = 20 # If results have not changed between X block checks, we think the application has died


class NodeConfiguration(object):
    '''Trivial class to describe how many nodes we want for Spark, how many for RADOS-Ceph, and what nodes will serve what purpose in the Ceph cluster.'''
    def __init__(self, num_spark_nodes, ceph_config):
        self._spark_nodes = num_spark_nodes
        self._ceph_config = ceph_config

    @property
    def num_spark_nodes(self):
        return self._spark_nodes

    @property
    def num_ceph_nodes(self):
        return len(self._ceph_config)

    @property
    def ceph_config(self):
        return self._ceph_config

    def __len__(self):
        return self.num_spark_nodes+self.num_ceph_nodes


class CephConfiguration(object):
    '''Configuration describing what the Ceph cluster should look like, by specifying designations'''

    def __init__(self, designations):
        '''Initializes a Configuration object.
        Args:
            designations (list(list(rados_deploy.Designation))): List of designations that some unspecified node should have.
                                                           The mapping of configurations onto specific nodes is not handled by this system.'''
        if any(x for x in designations if not x):
            raise ValueError('Designations contains empty or "None" designation-list: {}'.format(designations))
        self.designations = designations


    def __eq__(self, other):
        if not isinstance(other, CephConfiguration):
            return False
        return self.designations = other.designations

    def __len__(self):
        return len(self.designations)