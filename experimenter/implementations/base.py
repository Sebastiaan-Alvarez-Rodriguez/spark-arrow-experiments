from enum import Enum
import time

from rados_deploy import data

import experiments.util as eu
import util.fs as fs
import util.location as loc
from util.printer import *

from internal.data import data_generate as _data_generate 
from internal.configuration import CephConfiguration

def _default_node_configuration():
    return (8,CephConfiguration(
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD]))


class ExperimentBase(object):
    def __init__(self):
        # Cluster spawning params
        self.reserve_time = '10:00:00'

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
        if len(reservation) > max(x+y for x,y in self.nodes):
            printw('Reservation size ({} nodes) is more than we will need at most for this experiment ({} Spark-nodes + {} Ceph-nodes).'.format(len(reservation), *max(self.nodes, key=lambda x: x[0]+x[1])))

        if not self.nodes:
            printe('Experiment does not specify any node configurations!')
            return False

        for spark_nodes, ceph_config in self.nodes:
            ceph_nodes = len(ceph_config)

