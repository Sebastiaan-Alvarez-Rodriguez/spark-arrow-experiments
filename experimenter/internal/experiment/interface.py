import abc

def get_experiment():
    '''Implement this function in your experiment, make it return your experiment class'''
    raise NotImplementedError

class ExperimentInterface(metaclass=abc.ABCMeta):
    '''This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to implement the functions here.'''

    class_id = 9606858520421092931

    def get_configs(self):
        '''Get experiment configs.
        Returns:
            list(internal.experiment.ExperimentConfiguration), containing all different setups we want to experiment with.'''
        raise NotImplementedError

    def on_start(self, config, nodes_spark, nodes_ceph, num_experiment, amount_experiments):
        '''Function hook, called when we start an experiment. This function is called after resources have been allocated.
        Args:
            config (internal.experiment.ExperimentConfiguration): Current experiment configuration.
            nodes_spark (tuple(metareserve.Node, list(metareserve.Node))): Spark master, Spark worker nodes.
            nodes_ceph (tuple(metareserve.Node, list(metareserve.Node))): Ceph admin, Ceph other nodes. Note: The extra_info attribute of each node tells what designations it hosts with the "designations" key. 
            num_experiment (int): Current experiment run number (e.g. if we now run 4/10, value will be 4).
            amount_experiments (int): Total amount of experiments to run. (e.g. if we now run 4/10, value will be 10).

        Returns:
            `True` if we want to proceed with the experiment. `False` if we want to cancel it.'''
        return True

    def on_stop(self):
        return False

    @staticmethod
    def is_experiment(obj):
        try:
            return obj.class_id == ExperimentInterface.class_id
        except AttributeError as e:
            return False