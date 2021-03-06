from datetime import datetime

from rados_deploy import Designation

from experimenter.internal.experiment.execution.execution_interface import ExecutionInterface
import experimenter.internal.experiment.execution.functionstore.data_general as data_general
import experimenter.internal.experiment.execution.functionstore.distribution_general as distribution_general
import experimenter.internal.experiment.execution.functionstore.experiment_general as experiment_general
import experimenter.internal.experiment.execution.functionstore.spark as spark
import experimenter.internal.experiment.execution.functionstore.rados_ceph as rados_ceph

from experimenter.internal.experiment.interface import ExperimentInterface
from experimenter.internal.experiment.config import ExperimentConfigurationBuilder, ExperimentConfiguration, NodeConfiguration, CephConfiguration

import utils.fs as fs
import utils.location as loc
from utils.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so we can find it when loading.'''
    return CephExperiment()

        
def get_node_configuration():
    return NodeConfiguration(9, CephConfiguration(
        [[Designation.OSD, Designation.MON],
        [Designation.OSD, Designation.MON],
        [Designation.OSD, Designation.MON],
        [Designation.OSD, Designation.MGR],
        [Designation.OSD, Designation.MGR],
        [Designation.OSD, Designation.MDS],
        [Designation.OSD, Designation.MDS],
        [Designation.OSD, Designation.MDS]]))


# Performs experiment: Pushdown benefit, compares row selectivity pushdowns vs no pushdowns.
class CephExperiment(ExperimentInterface):
    '''This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to implement the functions here.'''

    def __init__(self):
        super(CephExperiment, self).__init__()


    def get_executions(self):
        ''''Get experiment ExecutionInterfaces.
        Returns:
            `iterable(internal.experiment.ExecutionInterfaces)`, containing all different setups we want to experiment with.'''
        data_queries = [
            'SELECT * FROM table WHERE total_amount > 69', #1% row selectivity, 100% column selectivity
            'SELECT * FROM table WHERE total_amount > 27', #10% row selectivity, 100% column selectivity
            'SELECT * FROM table WHERE total_amount > 17', #25% row selectivity, 100% column selectivity
            'SELECT * FROM table WHERE total_amount > 11', #50% row selectivity, 100% column selectivity
            'SELECT * FROM table WHERE total_amount > 8', #75% row selectivity, 100% column selectivity
            'SELECT * FROM table WHERE total_amount > 6', #90% row selectivity, 100% column selectivity
            'SELECT * FROM table', # 100% row selectivity, 100% column selectivity
        ]
        row_selectivities = [1, 10, 25, 50, 75, 90, 100]

        stripe = 128

        copy_multiplier, link_multiplier = (32, 128) #Total data size (for 128MB objects): 512GB
        timestamp = datetime.now().isoformat()

        configs = []
        for data_query, row_selectivity in zip(data_queries, row_selectivities):
            for offload in [True, False]:
                result_dirname = '{:03d}_{}'.format(row_selectivity, offload)
                configbuilder = ExperimentConfigurationBuilder()
                configbuilder.set('mode', '--arrow-only')
                configbuilder.set('runs', 21)
                configbuilder.set('batchsize', 1024)
                configbuilder.set('spark_driver_memory', '60G')
                configbuilder.set('spark_executor_memory', '60G')
                configbuilder.set('node_config', get_node_configuration())
                configbuilder.set('stripe', stripe)
                configbuilder.set('copy_multiplier', copy_multiplier)
                configbuilder.set('link_multiplier', link_multiplier)
                configbuilder.set('remote_result_dir', fs.join('~', 'results', 'exp_offload', str(timestamp), result_dirname))
                configbuilder.set('result_dir', fs.join(loc.result_dir(), 'exp_offload', str(timestamp), result_dirname))
                configbuilder.set('data_path', fs.join(loc.data_generation_dir(), 'jayjeet_128mb.pq'))
                configbuilder.set('data_query', '"{}"'.format(data_query))
                configbuilder.set('spark_conf_options', lambda conf: ExperimentConfiguration.base_spark_conf_options(conf)+[
                    'spark.arrowspark.pushdown.filters={}'.format(offload),
                    'spark.arrowspark.ceph.userados=True',
                ])
                config = configbuilder.build()
                configs.append(config)

        for idx, config in enumerate(configs):
            executionInterface = ExecutionInterface(config)
            executionInterface.register('distribute_func', distribution_general.distribute_default)
            experiment_general.register_default_experiment_function(executionInterface, idx, len(configs))
            experiment_general.register_default_result_fetch_function(executionInterface, idx, len(configs))
            rados_ceph.register_rados_ceph_deploy_data(executionInterface, idx, len(configs))
            spark.register_spark_functions(executionInterface, idx, len(configs))
            rados_ceph.register_rados_ceph_functions(executionInterface, idx, len(configs))
            yield executionInterface