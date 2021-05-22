class ExecutionInterface(object):
    def __init__(self, experiment, reservation):
        self._experiment = experiment
        self._reservation = reservation

        self.install_spark_func = None
        self.install_others_funcs = None
        self.start_spark_func = None
        self.start_others_funcs = None
        self.deploy_data_func = None
        self.stop_spark_func = None
        self.stop_others_funcs = None
        self.uninstall_spark_func = None
        self.uninstall_others_funcs = None

    @property
    def experiment(self):
        return self._experiment
    
    @property
    def reservation(self):
        return self._reservation
    

    def register(self, functype, func):
        '''Register a function/lambda to be executed during a stage. Stages:
        install_spark_func      : required, executed when we need to install Spark.
        install_others_funcs    : optional, installs others. Can register multiple functions, which will be executed in order of registering.
        start_spark_func        : required, starts Spark.
        start_others_funcs      : optional, starts others. Can register multiple functions, which will be executed in order of registering.
        deploy_data_func        : optional, deploys data.
        stop_spark_func         : required, stops Spark.
        stop_others_funcs       : optional, stops others. Can register multiple functions, which will be executed in order of registering.
        uninstall_spark_func    : required, uninstalls Spark.
        uninstall_others_funcs  : optional, uninstalls others. Can register multiple functions, which will be executed in order of registering.

        Args:
            functype (str): Stage to 
        '''
        if functype.endswith('func') or functype.endswith('funcs'):
            attr = getattr(self, functype) # Checks whether attribute exists
            if not callable(func):
                printe('Set function for attribute "{}" must be callable.'.format(functype))
                return False
            if functype.endswith('func'):
                setattr(self, functype, func)
            else: # We have a 'funcs' attr, should append
                attr.append(functype)
            return True
        else:
            printe('Cannot find stage "{}".'.format(functype))
            return False

    def execute(self):
        callables_available = [callable(x) for x in (self.install_spark_func, self.start_spark_func, self.stop_spark_func, self.uninstall_spark_func)]
        if not all(callables_available):
            printe('Not all required functions are set in Execution Interface.')
            callables_named = {self.install_spark_func: 'install_spark_func', self.start_spark_func: 'start_spark_func', self.stop_spark_func: 'stop_spark_func', self.uninstall_spark_func: 'uninstall_spark_func'}
            callables_missing = [v for k,v in callables_named.items() if not callable(k)]
            print('Missing:\n{}'.format('\n'.join('\t{}'.format(x) for x in callables_missing)))
            return False


        if not self._install_spark_func(self):
            printe('Could not install Spark.')
            return False


        if not self.start_spark_func(self):
            printe('Could not start Spark.')
            return False


        # TODO: Finish interface, attach hooks

    # def install_spark(self):
    #     pass

    # def install_others(self):
    #     pass

    # def start_spark(self):
    #     pass

    # def start_others(self):
    #     pass

    # def deploy_data(self):
    #     pass

    # def stop_spark(self):
    #     pass

    # def stop_others(self):
    #     pass

    # def uninstall_spark(self):
    #     pass

    # def uninstall_others(self):
    #     pass