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


# Performs experiment definiion 1 and 3.
class CephExperiment(ExperimentInterface):
    '''This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to implement the functions here.'''

    def __init__(self):
        super(CephExperiment, self).__init__()


    def get_executions(self):
        ''''Get experiment ExecutionInterfaces.
        Returns:
            `iterable(internal.experiment.ExecutionInterfaces)`, containing all different setups we want to experiment with.'''
        stripe = 64 # One file should have stripe size of 64MB
        multipliers = [(64, 16), (128, 8), (256, 4), (512, 2), (1024, 1)] #Total data size: 64 GB
        modes = ['--arrow-only', '--spark-only']
        timestamp = datetime.now().isoformat()

        configs = []
        for mode in modes:
            for (copy_multiplier, link_multiplier) in multipliers:
                configbuilder = ExperimentConfigurationBuilder()
                configbuilder.set('mode', mode)
                configbuilder.set('runs', 31)
                configbuilder.set('spark_driver_memory', '60G')
                configbuilder.set('spark_executor_memory', '60G')
                configbuilder.set('node_config', get_node_configuration())
                configbuilder.set('stripe', stripe)
                configbuilder.set('copy_multiplier', copy_multiplier)
                configbuilder.set('link_multiplier', link_multiplier)
                configbuilder.set('remote_result_dir', '~/results/ceph_experiment/{}_{}/{}'.format(copy_multiplier, link_multiplier, timestamp))

                config = configbuilder.build()
                configs.append(config) 

        for idx, config in enumerate(configs):
            executionInterface = ExecutionInterface(config)

            executionInterface.register('generate_data_funcs', lambda iface: data_general.generate_data_default(iface, idx, len(configs)))
            executionInterface.register('distribute_func', distribution_general.distribute_default)
            experiment_general.register_default_experiment_function(executionInterface, idx, len(configs))
            experiment_general.register_default_result_fetch_function(executionInterface, idx, len(configs))
            rados_ceph.register_rados_ceph_deploy_data(executionInterface, idx, len(configs))
            spark.register_spark_functions(executionInterface, idx, len(configs))
            rados_ceph.register_rados_ceph_functions(executionInterface, idx, len(configs))
            yield executionInterface


    def on_distribute(self):
        '''Function hook, called just before we distribute nodes to tasks, e.g. to perform a Spark/Ceph role.'''
        pass


    def on_install(self):
        '''Function hook, called just before we install the required platforms on the nodes allocated to a task.'''
        pass


    def on_start(self, config, nodes_spark, nodes_ceph, num_experiment, amount_experiments):
        '''Function hook, called just before we start an experiment. This function is called after resources have been allocated and prepared.
        Args:
            config (internal.experiment.ExperimentConfiguration): Current experiment configuration.
            nodes_spark (tuple(metareserve.Node, list(metareserve.Node))): Spark master, Spark worker nodes.
            nodes_ceph (tuple(metareserve.Node, list(metareserve.Node))): Ceph admin, Ceph other nodes. Note: The extra_info attribute of each node tells what designations it hosts with the "designations" key. 
            num_experiment (int): Current experiment run number (e.g. if we now run 4/10, value will be 4).
            amount_experiments (int): Total amount of experiments to run. (e.g. if we now run 4/10, value will be 10).

        Returns:
            `True` if we want to proceed with the experiment. `False` if we want to cancel it.'''
        return True


    def on_stop(self, config, nodes_spark, nodes_ceph, num_experiment, amount_experiments):
        '''Function hook, called when we stop an experiment. This function is called right after the experiment has completed.
        Args:
            config (internal.experiment.ExperimentConfiguration): Current experiment configuration.
            nodes_spark (tuple(metareserve.Node, list(metareserve.Node))): Spark master, Spark worker nodes.
            nodes_ceph (tuple(metareserve.Node, list(metareserve.Node))): Ceph admin, Ceph other nodes. Note: The extra_info attribute of each node tells what designations it hosts with the "designations" key. 
            num_experiment (int): Current experiment run number (e.g. if we now run 4/10, value will be 4).
            amount_experiments (int): Total amount of experiments to run. (e.g. if we now run 4/10, value will be 10).

        Returns:
            `True` if we want to proceed with the experiment. `False` if we want to cancel it.'''
        pass


    def on_uninstall(self):
        '''Function hook, called just before we uninstall platforms. Note: Not all tasks and platforms support uninstalling.'''
        pass


    def on_end(self):
        '''Function hook, called just before we end the experiment.'''
        pass