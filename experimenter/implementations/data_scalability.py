from rados_deploy import Designation

from internal.experiment.interface import ExperimentInterface
from experimenter.internal.experiment.config import ExperimentConfigurationBuilder, ExperimentConfiguration, NodeConfiguration, CephConfiguration

import utils.location as loc
from utils.printer import *

def get_experiment():
    '''Pass your defined experiment class in this function so we can find it when loading.'''
    return DataScalabilityExperiment()

        
def get_node_configuration():
    return NodeConfiguration(9, CephConfiguration(
        [[Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD, Designation.MGR, Designation.MDS],
        [Designation.MON, Designation.OSD]]))


class DataScalabilityExperiment(ExperimentInterface):
    '''This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to implement the functions here.'''

    def __init__(self):
        super(DataScalabilityExperiment, self).__init__()


    def get_configs(self):
        '''Get experiment configs.
        Returns:
            list(internal.experiment.ExperimentConfiguration), containing all different setups we want to experiment with.'''
        stripe = 64 # One file should have stripe size of 64MB
        data_multipliers = [1024, 2*1024, 4*1024, 8*1024, 16*1024] #Total data sizes: 64, 128, 256, 512, 1024 GB
        modes = ['--arrow-only', '--spark-only']
        for mode in modes:
            for x in data_multipliers:
                confbuilder = ExperimentConfigurationBuilder()
                confbuilder.set('mode', mode)
                confbuilder.set('runs', 11)
                confbuilder.set('node_config', get_node_configuration())
                confbuilder.set('stripe', stripe)
                confbuilder.set('data_multiplier', x)
                confbuilder.set('remote_result_dir', '~/results/data_scalability')
                yield confbuilder.build()


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