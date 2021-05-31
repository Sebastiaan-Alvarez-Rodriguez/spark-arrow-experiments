import time
import concurrent.futures

import metareserve
import spark_deploy
import rados_deploy

import experimenter.internal.data as data
from experimenter.internal.experiment.interface import ExperimentInterface
from experimenter.internal.remoto.util import get_ssh_connection as _get_ssh_connection
import experimenter.internal.experiment.blocker as blocker
import experimenter.internal.result.util as func_util
import utils.fs as fs
import utils.location as loc
from utils.printer import *


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def execute_single(name, experiment, reservation, skip_elements, exp_idx, exp_len):
    executions = list(experiment.get_executions())

    num_executions = len(executions)
    for idx, execution in enumerate(executions):
        printc('Executing "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions), Color.CAN)
        execution.reservation = reservation
        if not execution.execute(skip_elements):
            printw('Failed executing "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions))
        else:
            prints('Completed "{}" (which is experiment {}/{}): Execution {}/{}'.format(name, exp_idx+1, exp_len, idx+1, num_executions))
        if idx == 0: # TODO: Remove skipping elements later on
            skip_elements = {'spark': False, 'ceph': False, 'data': False}


def execute(experiment_mapping, reservation, skip_elements):
    '''Execute a series of experiments.
    Args:
        experiment_mapping (dict(str, module)): A mapping from experiment name to experiment module.
        reservation (metareserve.Reservation): Node reservation to use for executing experiments.

    Returns:
        `True` on success, `False` on failure.'''
    for name, experiment in experiment_mapping.items():
        if not ExperimentInterface.is_experiment(experiment):
            raise ValueError('Passed value is not a valid experiment: {} (type: {}).'.format(name, type(experiment)))

    # TODO: Verify if reservation is large enough for every experiment.
    # Note: This depends on the distribution function of each experiment.
    # In general: It fits as long as the amount of nodes >= amount of nodes needed by experiment for Spark+Ceph...

    for idx, (name, experiment) in enumerate(experiment_mapping.items()):
        print('Starting experiment "{}".'.format(name))
        execute_single(name, experiment, reservation, skip_elements, idx, len(experiment_mapping))
    return True