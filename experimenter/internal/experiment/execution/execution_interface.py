import metareserve

from utils.printer import *



def _func_valid(func):
    '''Returns `True` if given func is callable or a list of all callables, `False` otherwise.'''
    return callable(func) or (isinstance(func, list) and any(func) and all(callable(x) for x in func))



class ExecutionInterface(object):
    def __init__(self, config):
        self._config = config
        self._reservation = None
        self._distribution = None

        self.distribute_func = None
        self.install_spark_func = None
        self.install_others_funcs = []
        self.start_spark_func = None
        self.start_others_funcs = []
        self.generate_data_funcs = []
        self.deploy_data_func = None
        self.experiment_funcs = []
        self.result_fetch_funcs = []
        self.stop_spark_func = None
        self.stop_others_funcs = []
        self.uninstall_spark_func = None
        self.uninstall_others_funcs = []


    @property
    def config(self):
        return self._config

    @property
    def reservation(self):
        return self._reservation

    @reservation.setter
    def reservation(self, value):
        if self._reservation == None:
            if isinstance(value, metareserve.Reservation):
                self._reservation = value
            else:
                raise RuntimeError('Require reservation of type metareserve.Reservation. Trying to set reservation of inccorrect type: {}'.format(type(value)))
        else:
            raise RuntimeError('Reservation is already set: {}'.format(self._reservation))


    @property
    def distribution(self):
        return self._distribution


    def register(self, functype, func):
        '''Register a function/lambda to be executed during a stage. Stages:
        distribute_func         : required, picks nodes to run Spark and others. 
                                  Must return `bool, dict(str, list(metareserve.Node))`: First value indicates success, second value is a mapping from string key to reserved nodes.
                                  A key named 'spark' (lowercase) must exist.
        install_spark_func      : required, executed when we need to install Spark.
        install_others_funcs    : optional, installs others. Can register multiple functions, which will be executed in order of registering.
        start_spark_func        : required, starts Spark.
        start_others_funcs      : optional, starts others. Can register multiple functions, which will be executed in order of registering.
        generate_data_funcs     : optional, generates data. Can register multiple functions, which will be executed in order of registering.
        deploy_data_func        : optional, deploys data.
        experiment_funcs        : required, performs experiment. Can register multiple functions, which will be executed in order of registering.
        result_fetch_funcs      : optional, fetches results. Can register multiple functions, which will be executed in order of registering.
        stop_spark_func         : required, stops Spark.
        stop_others_funcs       : optional, stops others. Can register multiple functions, which will be executed in order of registering.
        uninstall_spark_func    : optional, uninstalls Spark.
        uninstall_others_funcs  : optional, uninstalls others. Can register multiple functions, which will be executed in order of registering.

        Every function/lambda registered must take exactly 1 argument, which is this interface. This can be used to fetch the experiment and reservation objects.
        If a function with other/more arguments must be registered, use a lambda to work between the call, e.g:
        ```
        def custom_function(some_int, reservation, greetings_str):
            ...
        some_int = 10
        exp_interface.register(lambda interface: custom_function(some_int, interface.reservation, "Hi world!"))
        ```

        Args:
            functype (str): Stage to register function for. Names of valid stages are listed right above here.
            func (callable): Function/lambda to register.

        Returns:
            `True` on success, `False` on failure.'''
        if functype.endswith('func') or functype.endswith('funcs'):
            attr = getattr(self, functype) # Checks whether attribute exists
            if not callable(func):
                printe('Set function for attribute "{}" must be callable.'.format(functype))
                return False
            if functype.endswith('func'):
                setattr(self, functype, func)
            else: # We have a 'funcs' attr, should append
                attr.append(func)
            return True
        else:
            printe('Cannot find stage "{}".'.format(functype))
            return False


    def execute(self, skip_elements):
        '''Executes experiment setup, calling registered methods as needed.
        Returns:
            `True` on successful execution, `False` otherwise.'''
        callables_named = {
            'distribute_func': self.distribute_func,
            'install_spark_func': self.install_spark_func,
            'start_spark_func': self.start_spark_func,
            'experiment_funcs': self.experiment_funcs,
            'stop_spark_func': self.stop_spark_func}
        if not all(_func_valid(x) for x in callables_named.values()):
            printe('Not all required functions are set to valid values in Execution Interface.')
            callables_missing = {k:v for k,v in callables_named.items() if not _func_valid(v)}
            print('Problem(s):\n{}'.format('\n'.join('\t{} (value: {})'.format(k, v) for k,v in callables_missing.items())))
            return False

        retval, *others = self.distribute_func(self)
        if not retval:
            printe('Could not distribute nodes.')
            return False
        if not any(others):
            printe('Distribution function did not return a distribution.')
            return False
        if not isinstance(others[0], dict):
            raise RuntimeError('Distribution has to be a dict, encountered "{}": {}'.format(type(others[0]), others[0]))
        if not 'spark' in others[0]:
            raise RuntimeError('Distribution function "{}" produced distribution without required "spark" key.'.format(self.distribute_func.__name__))
        self._distribution = others[0]


        nodes = set(self.reservation.nodes)
        nodes_encountered = set()
        for k,v in self._distribution.items():
            print('{} "{}" nodes:\n{}'.format(len(v), k, ''.join('\t{}\n'.format(x) for x in v)))
            nodes_encountered = nodes_encountered.union(v)
        print('Total number of used nodes (each node counted only once): {}.\n'.format(len(nodes_encountered)))

        unused_nodes = nodes.difference(nodes_encountered)
        if any(unused_nodes):
            printw('Found {} unused nodes:\n{}'.format(len(unused_nodes), ''.join('\t{}\n'.format(x) for x in unused_nodes)))


        if not skip_elements['spark']:
            print('Installing Spark ({} nodes)...'.format(len(self.distribution['spark'])))
            if not self.install_spark_func(self):
                printe('Could not install Spark.')
                return False
        if not skip_elements['ceph']:
            if any(self.install_others_funcs):
                print('Installing {} other components...'.format(len(self.install_others_funcs)))
            for idx, x in enumerate(self.install_others_funcs):
                if not x(self):
                    printe('Could not execute installation function {}/{}: {}'.format(idx+1, len(self.install_others_funcs), x.__name__))
                    return False

        if not skip_elements['spark']:
            print('Starting Spark ({} nodes)...'.format(len(self.distribution['spark'])))
            if not self.start_spark_func(self):
                printe('Could not start Spark.')
                return False
        if not skip_elements['ceph']:
            if any(self.start_others_funcs):
                print('Starting {} other components...'.format(len(self.start_others_funcs)))
            for idx, x in enumerate(self.start_others_funcs):
                if not x(self):
                    printe('Could not execute start function {}/{}: {}'.format(idx+1, len(self.start_others_funcs), x.__name__))
                    return False

        if not skip_elements['data']:
            if any(self.generate_data_funcs):
                print('Generating data ({} functions)...'.format(len(self.generate_data_funcs)))
            for idx, x in enumerate(self.generate_data_funcs):
                if not x(self):
                    printe('Could not execute data generation function {}/{}: {}'.format(idx+1, len(self.generate_data_funcs), x.__name__))
                    return False

            print('Deploying data...')
            if callable(self.deploy_data_func) and not self.deploy_data_func(self):
                printe('Could not deploy data.')
                return False

        print('Executing {} experiment function(s)...'.format(len(self.experiment_funcs)))
        for idx, x in enumerate(self.experiment_funcs):
            if not x(self):
                printe('Could not execute experiment function {}/{}: {}'.format(idx+1, len(self.experiment_funcs), x.__name__))
                return False

        if any(self.result_fetch_funcs):
            print('Aggregating results ({} functions)...'.format(len(self.result_fetch_funcs)))
            for idx, x in enumerate(self.result_fetch_funcs):
                if not x(self):
                    printe('Could not execute result fetch function {}/{}: {}'.format(idx+1, len(self.result_fetch_funcs), x.__name__))
                    return False


        print('Stopping Spark ({} nodes)...'.format(len(self.distribution['spark'])))
        if not self.stop_spark_func(self):
            printe('Could not stop Spark.')
            return False

        if any(self.stop_others_funcs):
            print('Stopping {} other components...'.format(len(self.stop_others_funcs)))
        for idx, x in enumerate(self.stop_others_funcs):
            if not x(self):
                printe('Could not execute stop function {}/{}: {}'.format(idx+1, len(self.stop_others_funcs), x.__name__))
                return False

        # print('Uninstalling Spark ({} nodes)...'.format(len(self.distribution['spark'])))
        # if not self.uninstall_spark_func(self):
        #     printe('Could not uninstall Spark.')
        #     return False

        # for idx, x in enumerate(self.uninstall_others_funcs):
        #     if not x(self):
        #         printe('Could not execute uninstall function {}/{}: {}'.format(idx+1, len(self.uninstall_others_funcs), x.__name__))
        #         return False
        return True