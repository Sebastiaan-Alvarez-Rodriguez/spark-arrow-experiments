import time
from collections import OrderedDict
import concurrent.futures

import metareserve
import spark_deploy
import rados_deploy

import experimenter.internal.data as data
from experimenter.internal.experiment.interface import ExperimentInterface
import experimenter.internal.experiment.blocker as blocker
import experimenter.internal.result.util as func_util
from experimenter.optimizer.optimizer import OptimizationConfig, optimize
import utils.fs as fs
import utils.location as loc
from utils.printer import *


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def _print_distribution(execution_interface):
    _nodes = set(execution_interface.reservation.nodes)
    _nodes_encountered = set()
    for k,v in execution_interface.distribution.items():
        print('{} "{}" nodes:\n{}'.format(len(v), k, ''.join('\t{}\n'.format(x) for x in v)))
        _nodes_encountered = _nodes_encountered.union(v)
    print('Total number of used nodes (each node counted only once): {}.\n'.format(len(_nodes_encountered)))

    _unused_nodes = _nodes.difference(_nodes_encountered)
    if any(unused_nodes):
        printw('Found {} unused nodes:\n{}'.format(len(_unused_nodes), ''.join('\t{}\n'.format(x) for x in _unused_nodes)))


def _install_spark(execution_interface):
    print('Installing Spark ({} nodes)...'.format(len(execution_interface.distribution['spark'])))
    if not execution_interface.install_spark_func(execution_interface):
        printe('Could not install Spark.')
        return False
    return True    


def _install_others(execution_interface):
    if any(execution_interface.install_others_funcs):
        print('Installing {} other components...'.format(len(execution_interface.install_others_funcs)))
        for idx, x in enumerate(execution_interface.install_others_funcs):
            if not x(execution_interface):
                printe('Could not execute installation function {}/{}: {}'.format(idx+1, len(execution_interface.install_others_funcs), x.__name__))
                return False
    return True


def _start_spark(execution_interface):
    print('Starting Spark ({} nodes)...'.format(len(execution_interface.distribution['spark'])))
    if not execution_interface.start_spark_func(execution_interface):
        printe('Could not start Spark.')
        return False
    return True


def _start_others(execution_interface):
    if any(execution_interface.start_others_funcs):
        print('Starting {} other components...'.format(len(execution_interface.start_others_funcs)))
        for idx, x in enumerate(execution_interface.start_others_funcs):
            if not x(execution_interface):
                printe('Could not execute start function {}/{}: {}'.format(idx+1, len(execution_interface.start_others_funcs), x.__name__))
                return False
    return True


def _generate_data(execution_interface):
    if any(execution_interface.generate_data_funcs):
        print('Generating data ({} functions)...'.format(len(execution_interface.generate_data_funcs)))
        for idx, x in enumerate(execution_interface.generate_data_funcs):
            if not x(execution_interface):
                printe('Could not execute data generation function {}/{}: {}'.format(idx+1, len(execution_interface.generate_data_funcs), x.__name__))
                return False
    return True


def _deploy_data(execution_interface):
    if callable(execution_interface.deploy_data_func):
        print('Deploying data...')
        if not execution_interface.deploy_data_func(execution_interface):
            printe('Could not deploy data.')
            return False
    return True


def _experiment(execution_interface):
    print('Executing {} experiment function(s)...'.format(len(execution_interface.experiment_funcs)))
    for idx, x in enumerate(execution_interface.experiment_funcs):
        if not x(execution_interface):
            printe('Could not execute experiment function {}/{}: {}'.format(idx+1, len(execution_interface.experiment_funcs), x.__name__))
            return False
    return True


def _result_fetch(execution_interface):
    if any(execution_interface.result_fetch_funcs):
        print('Aggregating results ({} functions)...'.format(len(execution_interface.result_fetch_funcs)))
        for idx, x in enumerate(execution_interface.result_fetch_funcs):
            if not x(execution_interface):
                printe('Could not execute result fetch function {}/{}: {}'.format(idx+1, len(execution_interface.result_fetch_funcs), x.__name__))
                return False
    return True


def _stop_spark(execution_interface):
    print('Stopping Spark ({} nodes)...'.format(len(execution_interface.distribution['spark'])))
    if not execution_interface.stop_spark_func(execution_interface):
        printe('Could not stop Spark.')
        return False
    return True


def _stop_others(execution_interface):
    if any(execution_interface.stop_others_funcs):
        print('Stopping {} other components...'.format(len(execution_interface.stop_others_funcs)))
        for idx, x in enumerate(execution_interface.stop_others_funcs):
            if not x(execution_interface):
                printe('Could not execute stop function {}/{}: {}'.format(idx+1, len(execution_interface.stop_others_funcs), x.__name__))
                return False
    return True


def execute(execution_interface, unchanged_distributions=False):
    '''Executes experiment setup, calling registered methods as needed.
    Returns:
        `True` on successful execution, `False` otherwise.'''
    if not execution_interface.validate():
        return False

    _print_distribution(execution_interface)

    if not _install_spark(execution_interface):
        return False
    if not _install_others(execution_interface):
        return False

    if not _start_spark(execution_interface):
        return False
    if not _start_others(execution_interface):
        return False

    if not _generate_data(execution_interface):
        return False
    if not _deploy_data(execution_interface):
        return False

    if not _experiment(execution_interface):
        return False

    if not _result_fetch(execution_interface):
        return False
    
    if not _stop_spark(execution_interface):
        return False
    if not _stop_others(execution_interface):
        return False

    return True


def experiment_execute_single(name, experiment, reservation, exp_idx, exp_len):
    executions = list(experiment.get_executions())

    num_executions = len(executions)
    for idx, execution_interface in enumerate(executions):
        printc('Executing "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions), Color.CAN)
        execution_interface.reservation = reservation
        if not execute(execution_interface):
            printw('Failed executing "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions))
        else:
            prints('Completed "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions))


def experiment_execute(experiment_mapping, reservation):
    '''Execute a series of experiments.
    Args:
        experiment_mapping (OrderedDict(str, module)): A mapping from experiment name to experiment module.
        reservation (metareserve.Reservation): Node reservation to use for executing experiments.

    Returns:
        `True` on success, `False` on failure.'''
    for name, experiment in experiment_mapping.items():
        if not ExperimentInterface.is_experiment(experiment):
            raise ValueError('Passed value is not a valid experiment: {} (type: {}).'.format(name, type(experiment)))

    # TODO: Verify if reservation is large enough for every experiment.
    # Note: This depends on the distribution function of each experiment.
    # In general: It fits as long as the amount of nodes >= amount of nodes needed by experiment for Spark+Ceph...


    group_global = OrderedDict((k,v) for k,v in experiment_mapping.items() if v.accept_global_optimizations())
    group_nonglobal = OrderedDict((k,v) for k,v in experiment_mapping.items() if not v.accept_global_optimizations())

    import itertools
    all_optimizations = []
    for x in experiment_mapping.values():
        if x.accept_global_optimizations():
            all_optimizations += x.get_optimizationconfig().optimization_iterate()

    all_optimizations = list(OrderedDict.fromkeys(all_optimizations))
    global_conf = OptimizationConfig(optimizations=all_optimizations, comperator=next(iter(experiment_mapping.values())).get_optimizationconfig().comperator)
    print('Found {} unique optimizations:\n{}'.format(len(global_conf), '\n'.join('    {}'.format(x.name) for x in global_conf.optimization_iterate())))

    # Constructs: ('exp01', exec), ('exp01', exec2)...
    global_interfaces = list(itertools.chain((x, y) for y in x.get_executions()) for x in group_global.values())
    optimized = optimize(global_interfaces, global_conf)

    for k,v in group_nonglobal.items(): # Optimizes non-global-allowing executions.
        group_nonglobal[k] = optimize(v.get_executions(), v.get_optimizationconfig())
        optimized += [(k,v) for v in group_nonglobal[k]]

    encountered_dict = {name: 0 for name in experiment_mapping}
    for exp_name, execution_interface in optimized:
        print('Processing experiment={} (executor={}/{})'.format(encountered_dict[exp_name]+1, len(experiment_mapping[exp_name])))
        pass
    # exp_len = len(experiment_mapping)
    # for exp_idx, (name, experiment) in enumerate(experiment_mapping.items()):
    #     executions = list(experiment.get_executions())

    #     num_executions = len(executions)
    #     for idx, execution_interface in enumerate(executions):
    #         printc('Executing "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions), Color.CAN)
    #         execution_interface.reservation = reservation
    #         if not execute(execution_interface):
    #             printw('Failed executing "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions))
    #         else:
    #             prints('Completed "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions))

    return True