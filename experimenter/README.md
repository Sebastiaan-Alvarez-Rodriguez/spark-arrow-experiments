# Experimenter
Experimentation framework.

## Concept
The general cycle all experiments follow:
 1. Divide nodes into pools.
 2. Install requirements.
 3. Start frameworks.
 4. Deploy data.
 5. Submit Spark application.
 6. Aggregate results.
 7. Stop frameworks

We formalized this concept inside this framework.
Experiments define how they give shape to this cycle by registering functions with the execution framework.
The execution framework triggers these functions at the correct time in the cycle.

We provided many default function implementations, to be able to perform automated experiments.

To use the experimentation framework, provided in the [`expermenter`](/experimenter/) directory, use:
```bash
python3 experimenter/entrypoint.py -h
```

## Adding Experiments
An experiment definition looks like:
```python

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



class CephExperiment(ExperimentInterface):
    '''This interface provides hooks, which get triggered on specific moments in deployment execution.
    It is your job to implement the functions here.'''

    def __init__(self):
        super(CephExperiment, self).__init__()


    def get_executions(self):
        ''''Get experiment ExecutionInterfaces.
        Returns:
            `iterable(internal.experiment.ExecutionInterfaces)`, containing all different setups we want to experiment with.'''
 
        configs = []
        for mode in ['--arrow-only', '--spark-only']:
            result_dirname = 'cp{}_ln{}'.format(copy_multiplier, link_multiplier)
            configbuilder = ExperimentConfigurationBuilder()
            configbuilder.set('batchsize', 1024)
            configbuilder.set('mode', mode)
            configbuilder.set('runs', 21)
            configbuilder.set('spark_driver_memory', '60G')
            configbuilder.set('spark_executor_memory', '60G')
            configbuilder.set('node_config', NodeConfiguration(9, CephConfiguration(
                [[Designation.OSD, Designation.MON],
                [Designation.OSD, Designation.MON],
                [Designation.OSD, Designation.MON],
                [Designation.OSD, Designation.MGR],
                [Designation.OSD, Designation.MGR],
                [Designation.OSD, Designation.MDS],
                [Designation.OSD, Designation.MDS],
                [Designation.OSD, Designation.MDS]]))
            )
            configbuilder.set('remote_result_dir', fs.join('~', 'results', 'exp_data', str(timestamp), result_dirname))
            configbuilder.set('result_dir', fs.join(loc.result_dir(), 'exp_data', str(timestamp), result_dirname))
            configbuilder.set('data_path', fs.join(loc.data_generation_dir(), 'jayjeet_128mb.pq'))
            configbuilder.set('spark_conf_options', lambda conf: ExperimentConfiguration.base_spark_conf_options(conf)+[
                'spark.arrowspark.pushdown.filters=True',
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
```

The most important function here is `get_executions`, which returns a number of `ExecutionInterface`, as a list or generator.
The framework will execute the experiments in the order they are provided.

Essentially, `get_executions` consists of 2 parts:
 1. The config loop (first `for` loop), where we create a configuration for each of the executions we want to use.
 2. The execution interface loop (second `for` loop), where we create an `ExecutionInterface` and register functions to use during experimentation.

The configuration contains options, such as the `NodeConfiguration`, which contains the layout of the nodes to spawn, and the remote result dir, which indicates where results should be stored.
Most options are specifically made for experimenting with out systems, but they can be changed, added to, or removed.
Configuration options are used throughout the experimentation framework's internals, and for many plugin functions.

The ExecutionInterface provides a way for users to register their own functions during stages.
Functions are called when the stage comes up they registered for, and are executed in order of specifying.
Functions have access to the configuration object that was passed to construct the `ExecutionInterface`.
They are free to modify the contents of the configuration in any way.


### Functions
Functions can be registered for the following events:
 1. `distribute_func`: Distributes nodes between clusters.
 2. `install_spark_func`: Installs Spark.
 3. `install_others_funcs`: Install other components for this experiment.
 4. `start_spark_func`: Starts Spark.
 5. `start_others_funcs`: Start other components for this experiment.
 6. `generate_data_funcs`: Generate data.
 7. `deploy_data_func`: Deploy data.
 8. `experiment_funcs`: Execute experiment.
 9. `result_fetch_funcs`: Fetch results.
10. `stop_spark_func`: Stops Spark.
11. `stop_others_funcs`: Stop other components for this experiment.
12. `uninstall_spark_func`: Uninstalls Spark.
13. `uninstall_others_funcs`: Uninstalls other components for this experiment.
The events ending on `_funcs` can have 0 or more functions registered to them.
The events ending on `_func` can have 0 or 1 functions registered to them.
Some functions ending on `_func` require a registered function.

Each registered function is called with the `ExecutionInterface` as argument.
The ExecutionInterface holds a reference to the `config`, and to the `reservation` of nodes.

A series of default functions are available at [`/experimenter/internal/experiment/functionstore/`](/experimenter/internal/experiment/functionstore/).
Each function clearly states which configuration options it expects to find.

### Config
The config we use for each `ExecutionInterface` contains many properties to define how registered functions work.
Each function clearly states which configuration options it expects to find.
We provide a default configuration with a builder pattern, which allows to set and create any option in a clean way.
The 