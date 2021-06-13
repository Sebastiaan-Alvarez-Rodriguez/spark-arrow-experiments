import abc
from experimenter.optimizer.optimizer import OptimizationConfig
from experimenter.optimizer.impl.data import DataOptimization
from experimenter.optimizer.impl.distribution import DistributionOptimization

def get_experiment():
    '''Implement this function in your experiment, make it return your experiment class'''
    raise NotImplementedError

class ExperimentInterface(metaclass=abc.ABCMeta):
    '''Base Experiment interface. Every experiment implements this base class, and this framework executes the experiments.'''
    class_id = 9606858520421092931

    def get_executions(self):
        '''Get experiment ExecutionInterfaces.
        Returns:
            `iterable(internal.experiment.ExecutionInterfaces)`, containing all different setups we want to experiment with.'''
        raise NotImplementedError


    def get_optimizationconfig(self):
        '''Get optimizationconfig, which tells which optimizations to apply between executions of this experiment.'''
        priority_options = { # Note: Highest priority goes first. Also note: Priorities must be unique to ensure consistent behaviour.
            DataOptimization(): 1,
            DistributionOptimization(): 2,
        }
        return OptimizationConfig(optimizations=list(priority_options.keys()), comperator=lambda x,y: priority_options[x] > priority_options[y])


    def accept_global_optimizations(self):
        '''If set, this experiment allows to use global optimizations from other experiments. This is generally a good idea to enable,
        as it enables cross-experiment optimizations, and could add other useful optimization groups.
        Example: If this experiment (A) defines to use optimizations A0, A1, and another experiment B defines optimization B0, then we use optimization A0,A1,B0 for A.
        If we do not accept global optimizations, we only apply A0 and A1 to A.'''
        return True

    @staticmethod
    def is_experiment(obj):
        try:
            return obj.class_id == ExperimentInterface.class_id
        except AttributeError as e:
            return False