from enum import Enum
import time

import metareserve
import spark_deploy
import rados_deploy

import experiments.util as eu
import util.fs as fs
import util.location as loc
from util.printer import *

from internal.data import data_generate as _data_generate 
from internal.configuration import NodeConfiguration, CephConfiguration

def _default_node_configuration():
    return NodeConfiguration(8, CephConfiguration(
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD]))


  

class ExperimentBase(object):
    def __init__(self):
        # Spark cluster options
        self.spark_silent = False
        
        # RADOS-Ceph cluster options
        self.ceph_silent = False
        self.ceph_compile_cores = 16
        self.ceph_mountpoint_path = '/mnt/cephfs'
        #Shared cluster options
        self.silent = False # Overrides both spark_silent and ceph_silent if set to `True`.
        self.key_path = '~/.ssh/geni.rsa'
        self.spark_workdir

        # Data deployment params
        # self.first_time_force = False # Force to generate the data, even if the directory exists, when we launch a cluster for the first time.

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
        # self.partitions_per_nodes = [16] # One DAS5 node has 16 physical, 32 logical cores, we use an X amount of partitions per physical core
        self.nodes = [_default_node_configuration()] # Formatted as (Spark-nodes, Ceph-nodes). Note: Number of Ceph-nodes must be at least 3.
        self.amount = 600000000
        self.amount_multipliers = [64] # makes number of rows this factor larger using symlinks
        self.extensions = ['pq']
        self.compressions = ['uncompressed']
        self.compute_columns = [4]
        self.kinds = ['df']
        self.rbs = [8192]
        self.test_modes = ['--arrow-only', '--spark-only']
        self.custom_rb_func = None #Function to override rbs for experiments with variable amounts of input data

        self.runs = 31 # We run our selfementation and the Spark baseline selfementation X times
        self.retries = 2 # If our application dies X times, we stop trying and move on
        self.appl_sleeptime = 30 # Sleep X seconds between checks
        self.appl_dead_after_tries = 20 # If results have not changed between X block checks, we think the application has died


    def transform_request(self, reservation):
        for x in self.nodes


    def launch(self, reservation):
        if len(reservation) > max(len(x) for x in self.nodes):
            printw('Reservation size ({} nodes) is more than we will need at most for this experiment ({} Spark-nodes + {} Ceph-nodes).'.format(len(reservation), *max(self.nodes, key=lambda x: len(x))))

        if not self.nodes:
            printe('Experiment does not specify any node configurations!')
            return False

        sorted_configs = sorted(self.nodes, key=lambda x: x.num_spark_nodes) # TODO: Better to sort on ceph configs: Keep the same ones together, so we can keep ceph running maximally long
        reservation_snapshot = list(reservation.nodes)
        for idx, conf in enumerate(sorted_configs):
            num_spark_nodes = conf.num_spark_nodes
            num_ceph_nodes = conf.num_ceph_nodes

            spark_nodes = reservation_snapshot[num_spark_nodes]
            ceph_nodes = reservation_snapshot[num_spark_nodes:num_spark_nodes+num_ceph_nodes]
            
            # if idx > 0 and sorted_configs[idx-1].num_spark_nodes == num_spark_nodes: #TODO: can skip installation, only have to stop & restart
            if not spark_deploy.install(metareserve.Reservation(spark_nodes), key_path=self.key_path, silent=self.spark_silent or self.silent):
                printe('Could not install Spark.') #TODO: Indicate at what point in the iterations the error happened
                return False

            if not spark_deploy.start(metareserve.Reservation(spark_nodes), key_path=self.key_path, worker_workdir=self.spark_workdir, silent=self.spark_silent or self.silent):
                printe('Could not start Spark.') #TODO: Indicate at what point in the iterations the error happened
                return False


            # if idx > 0 and sorted_configs[idx-1].ceph_config == conf.ceph_config: # TODO: Can keep ceph running as-is, and just replace some data.
            # Note: Must make sure to use the previous ceph nodes. Due to changing spark reservation sizes, this is now not the case.
            if not rados_deploy.install(metareserve.Reservation(ceph_nodes), key_path=self.key_path, silent=self.ceph_silent or self.silent, cores=self.ceph_compile_cores):
                printe('Could not install RADOS-Ceph.')
                return False

            if not rados_deploy.start(metareserve.Reservation(ceph_nodes), key_path=self.key_path, mountpoint_path=self.ceph_mountpoint_path, silent=self.ceph_silent or self.silent):
                printe('Could not start RADOS-Ceph.')
                return False

            if not rados_deploy.deploy(metareserve.Reservation(ceph_nodes), paths=DATA_PATHS, key_path=self.key_path, stripe=DATA_STRIPE, multiplier=DATA_MULTIPLIER, mountpoint_path=self.ceph_mountpoint_path, silent=self.ceph_silent or self.silent):
                printe('Data deployment on RADOS-Ceph failed.')
                return False
            # TODO: Some computation should happen here.
            prints('Super hardcore computation completed!')

            if not rados_deploy.stop(metareserve.Reservation(ceph_nodes), key_path=self.key_path, mountpoint_path=self.ceph_mountpoint_path, silent=self.ceph_silent or self.silent):
                printe('Could not stop RADOS-Ceph deployment.')
                return False

            if not spark_deploy.stop(metareserve.Reservation(spark_nodes), key_path=self.key_path, worker_workdir=self.spark_workdir, silent=self.spark_silent or self.silent):
                printe('Could not stop Spark deployment.')

            return True # Test completion. TODO: Remove
        return True