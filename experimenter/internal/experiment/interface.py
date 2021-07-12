import abc

def get_experiment():
    '''Implement this function in your experiment, make it return your experiment class'''
    raise NotImplementedError

class ExperimentInterface(metaclass=abc.ABCMeta):
    '''This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to implement the functions here.'''

    class_id = 9606858520421092931

    def get_executions(self):
        '''Get experiment ExecutionInterfaces.
        Returns:
            `iterable(internal.experiment.ExecutionInterfaces)`, containing all different setups we want to experiment with.'''
        raise NotImplementedError


    @staticmethod
    def is_experiment(obj):
        try:
            return obj.class_id == ExperimentInterface.class_id
        except AttributeError as e:
            return False