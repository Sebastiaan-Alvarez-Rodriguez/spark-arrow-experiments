class ExecutionInterface(object):
    def __init__(self, experiment, reservation):
        self._experiment = experiment
        self._reservation = reservation

        self.install_spark_func = None
        self.install_others_func = None
        self.start_spark_func = None
        self.start_others_func = None
        self.deploy_data_func = None
        self.stop_spark_func = None
        self.stop_others_func = None
        self.uninstall_spark_func = None
        self.uninstall_others_func = None

    @property
    def experiment(self):
        return self._experiment
    
    @property
    def reservation(self):
        return self._reservation
    

    def execute(self):
        if not (callable(self._install_spark_func) and self._install_spark_func(self)):
            printe('Could not install Spark.')
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