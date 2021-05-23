import utils.fs as fs
import utils.location as loc
from rados_deploy import Designation

'''Configuration classes to define experiment behaviour.'''

def _default_node_configuration():
    return NodeConfiguration(9, CephConfiguration(
        [[Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD]]))


def _to_val(val_or_callable, *args, **kwargs):
    if callable(val_or_callable):
        return val_or_callable(*args, **kwargs)
    return val_or_callable


class ExperimentConfiguration(object):
    '''Data class containing all kinds of configurable parameters for experiments. Create an instance of this class using `ExperimentConfigurationBuilder`.'''
    def __init__(self):
        # Argument params
        self.runs = 31
        self.kind = 'df'
        self.mode = '--arrow-only' # Pick from '--arrow-only, --spark-only'

        # Experiment params
        self.node_config = _default_node_configuration() # Must be a `NodeConfiguration`. Note: Number of Ceph-nodes must be at least 3.        
        self.tries = 2 # If our application dies X times, we stop trying and move on
        self.sleeptime = 30 # Sleep X seconds between checks
        self.dead_after_tries = 20 # If results have not changed between X block checks, we think the application has died
        # Unused experiment params
        self.eventlog_path = None  # Set this to an existing directory to make Spark history server logs
        self.flamegraph_time = None
        self.flamegraph_only_master = False
        self.flamegraph_only_worker = False

        # Spark cluster options
        self.spark_silent = False
        self.spark_start_stop_with_sudo = False # Use sudo to start and stop spark
        self.spark_submit_with_sudo = False # use 'sudo spark-submit ...' instead of 'spark-submit ...'
        self.spark_workdir = '~/spark_workdir'
        self.spark_force_reinstall = False
        self.spark_download_url = 'https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz'

        # RADOS-Ceph cluster options
        self.ceph_silent = False
        self.ceph_compile_cores = 16
        self.ceph_mountpoint_dir = '/mnt/cephfs'
        self.ceph_force_reinstall = False
        self.ceph_debug = False
        self.ceph_used = True # If set to False, we don't use Ceph and skip installing/booting/stopping.        

        #Shared cluster options
        self.silent = False # Overrides both `spark_silent` and `ceph_silent` if set to `True`.
        self.key_path = '~/.ssh/geni.rsa' # Key to use when connecting from our machine to them, remotely.

        # Data deployment params - Check all the possible parameters
        self.data_generator_name = 'num_generator'
        self.data_path = lambda conf: fs.join(loc.data_generation_dir(), '{}_{}_{:04}_{:06}'.format(_to_val(conf.data_format, conf), _to_val(conf.data_generator_name, conf), _to_val(conf.stripe, conf), _to_val(conf.data_multiplier, conf))) # Local data path.
        self.remote_data_dir = lambda conf: _to_val(conf.ceph_mountpoint_dir, conf) if _to_val(conf.ceph_used, conf) else '~/data'
        self.stripe = 64 # Generate a parquet file for a stripe-constraint of X MB.
        self.data_multiplier = 20 # makes dataset this factor larger using symlinks (default value multiplies to 64*20=1280MB).
        self.data_format = 'parquet'
        self.num_columns = 4
        self.data_gen_extra_args = None
        self.data_gen_extra_kwargs = None

        # Unused data deployment params
        self.compute_columns = 4

        # Application deployment params
        self.resultdir = '~/results' # Resultdir on the remote cluster.
        self.resultfile = lambda conf: '{}_{}_{:04}_{:06}.res_{}'.format(_to_val(conf.data_format, conf), _to_val(conf.data_generator_name, conf), _to_val(conf.stripe, conf), _to_val(conf.data_multiplier, conf), 'a' if 'arrow' in _to_val(conf.mode, conf) else 's')
        self.batchsize = 8192 # This sets the read chunk size in bytes, both for Spark and for our bridge. Tweaking this parameter is important.
        self.spark_application_type = 'java'
        self.spark_deploymode = 'cluster'
        self.spark_java_options = []
        # self.spark_java_options = ['-Dlog4j.configuration=file:{}'.format(fs.join(loc.get_metaspark_log4j_conf_dir(), 'driver_log4j.properties'))]
        self.spark_conf_options = lambda conf: [
            "'spark.driver.extraJavaOptions={}'".format('-Dfile={} -Dio.netty.allocator.directMemoryCacheAlignment=64'.format(fs.join(_to_val(conf.resultdir, conf), _to_val(conf.resultfile, conf)))),
            "'spark.executor.extraJavaOptions={}'".format('-Dfile={} -Dio.netty.allocator.directMemoryCacheAlignment=64'.format(fs.join(_to_val(conf.resultdir, conf), _to_val(conf.resultfile, conf)))),
            "'spark.driver.extraClassPath={}'".format(fs.join(_to_val(conf.remote_application_dir, conf), _to_val(conf.spark_application_path, conf))),
            "'spark.executor.extraClassPath={}'".format(fs.join(_to_val(conf.remote_application_dir, conf), _to_val(conf.spark_application_path, conf))),
            "'spark.sql.parquet.columnarReaderBatchSize={}'".format(_to_val(conf.batchsize, conf)),
        ]
        self.spark_application_args = lambda conf: '{} --path {} --result-path {} --format {} --num-cols {} --compute-cols {} -r {}'.format(_to_val(conf.kind, conf), _to_val(conf.ceph_mountpoint_dir, conf), fs.join(_to_val(conf.resultdir, conf), _to_val(conf.resultfile, conf)), _to_val(conf.data_format, conf), _to_val(conf.num_columns, conf), _to_val(conf.compute_columns, conf), _to_val(conf.runs, conf))
        self.spark_application_mainclass = 'org.arrowspark.benchmark.Benchmark'
        self.spark_extra_jars = []

        self.remote_application_dir = '~/application'
        self.spark_application_path = 'arrow-spark-benchmark-4.0-light.jar' # path to application on the remote. Executed on remote with CWD=`remote_application_dir`.
        self.local_application_paths = lambda conf: [fs.join(loc.application_dir(), _to_val(conf.spark_application_path, conf))] # paths on local machine to files/folders we want to have available when executing spark-submit. Should contain at least the application we want to submit. Data will be placed in `remote_application_dir` on remote.
        # TODO: Unused application deployment configuration parameters. Re-implement?
        self.offheap_memory = None #1024*1024*1 # 1 mb of off-heap memory per JVM. Set to None to disable offheap memory
        self.submit_opts = None
        self.shared_submit_opts = None


class ExperimentConfigurationBuilder(object):
    '''Simple builder object. Allows you to instantiate a class, change attributes, and finalize them using the `build` method.
    This builder allows users to set lambdas/callable functions as values.
    These get executed on configuration finalization, with the to-be-finalized class instance.'''
    def __init__(self, clazz=ExperimentConfiguration):
        self.instance = clazz()

    def set(self, name, value):
        '''Set any attribute of a `clazz` instance. 
        Note: You can assign lambdas/callable function as values.
        These callables must take 1 argument, to which the `clazz` instance will be passed.
        Warning: If your callables require other instance variables, you must explicitly check if they are callable (then you must call them to get value) or just values.
        Args:
            name (str): Name of the attribute to set.
            value (any type, callable): Value for the attribute to set. Can be callable with 1 argument.'''
        if name.startswith('_'):
            raise ValueError('Illegal set-call. Cannot set config attributes starting with underscores ("_"). Found name: {}'.format(name))
        setattr(self.instance, name, value)

    def build(self):
        '''Build an instance of `class`.'''
        config_attr_names = [x for x in dir(self.instance) if not x.startswith('_')]
        for x in config_attr_names:
            attr = getattr(self.instance, x)
            if callable(attr):
                setattr(self.instance, x, attr(self.instance))
        return self.instance


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
        return self.designations == other.designations

    def __len__(self):
        return len(self.designations)