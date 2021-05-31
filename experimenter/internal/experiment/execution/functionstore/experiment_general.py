import concurrent.futures

import metareserve
import spark_deploy

import experimenter.internal.experiment.blocker as blocker
from spark_deploy.internal.remoto.util import get_ssh_connection as _get_ssh_connection
import experimenter.internal.result.util as func_util

from experimenter.internal.experiment.execution.functionstore.util import get_user_home

import utils.fs as fs
from utils.printer import *

def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z

def _get_connection(config, node):
    ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
    if config.key_path:
        ssh_kwargs['IdentityFile'] = config.key_path
    return _get_ssh_connection(node.ip_public, silent=config.spark_silent or config.silent, ssh_params=_merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}))


def _get_connections(config, spark_nodes):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(spark_nodes)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if config.key_path:
            ssh_kwargs['IdentityFile'] = config.key_path

        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=config.spark_silent or config.silent, ssh_params=_merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']})) for x in spark_nodes}
        return {node: future.result() for node, future in futures_connection.items()}


def _submit_blocking(config, command, spark_nodes, spark_master_id, spark_connectionwrappers=None):
    '''Submits Spark command. Waits on completion by checking the amount of results gathered to this point.
    If the system appears to have crashed, we reboot it and make it continue.
    Args:
        config (ExperimentConfiguration): Configuration to read control parameters from.
        command (str): Command to provide to spark-submit.
        spark_nodes (list(metareserve.Node)): Nodes we run Spark on.
        spark_master_id (int): Node id of the Spark master node.
        spark_connectionwrappers (dict(metareserve.Node, RemotoSSHWrapper)): Open connections to Spark nodes.

    Returns:
        `True` if the run is complete and we collected enough data. `False` if the run crashed too many times.'''
    if spark_connectionwrappers == None:
        spark_connectionwrappers = _get_connections(config, spark_nodes)
    remote_result_loc = fs.join(config.remote_result_dir, config.remote_result_file)
    lines_needed = config.runs

    for _try in range(config.tries):
        if not spark_deploy.submit(metareserve.Reservation(spark_nodes), command, paths=config.local_application_paths, key_path=config.key_path, master_id=spark_master_id, use_sudo=config.spark_submit_with_sudo, silent=config.spark_silent or config.silent):
            printw('Could not submit application on remote. Used command: {}'.format(command))
            return False

        if config.spark_deploymode == 'client': # We know the driver is executed on the spark master node in client mode.
            driver_node_id = spark_master_id
        else: # We have to find the node that executes the driver in cluster mode.
            state, val = blocker.block_with_value(func_util.remote_file_find, args=(spark_connectionwrappers, remote_result_loc), return_val=True, sleeptime=10, dead_after_tries=3) 
            if state == blocker.BlockState.COMPLETE:
                driver_node_id = val[0]
                print('Found driver running on node_id={}'.format(driver_node_id))
            else:
                raise RuntimeError('Could not find results file on any node: {}'.format(remote_result_loc))

        driver_node = next(node for node, wrapper in spark_connectionwrappers.items() if node.node_id == driver_node_id)
        state, val = blocker.block_with_value(func_util.remote_count_lines, args=(spark_connectionwrappers[driver_node].connection, remote_result_loc, lines_needed, config.spark_silent or config.silent), return_val=True, sleeptime=config.sleeptime, dead_after_tries=config.dead_after_tries)
        if state == blocker.BlockState.COMPLETE:
            return True
        if state == blocker.BlockState.TIMEOUT:
            printw('System timeout detected. Current status: {}/{}'.format(val[0], config.runs))
            if val[0] == 0:
                printw('No runs have completed. Does the Spark code crash because of an error?')
            lines_needed += 1 # +1 because we need a new line for warming caches.
    return False


def experiment_deploy_default(interface, idx, num_experiments):
    config = interface.config
    spark_master_id = interface.spark_master_id
    spark_master_url = interface.spark_master_url

    spark_connectionwrappers = _get_connections(config, interface.distribution['spark'])
    homedir = get_user_home(list(spark_connectionwrappers.values())[0].connection)

    make_remote_abspath = lambda string: string.replace('~', homedir) 

    cmd_builder = spark_deploy.SubmitCommandBuilder(cmd_type=config.spark_application_type)
    cmd_builder.set_master(spark_master_url)
    cmd_builder.set_deploymode(config.spark_deploymode)
    cmd_builder.set_driver_memory(config.spark_driver_memory)
    cmd_builder.set_executor_memory(config.spark_executor_memory)
    cmd_builder.add_java_options(*[make_remote_abspath(x) for x in config.spark_java_options])
    cmd_builder.set_application(config.spark_application_path)
    cmd_builder.add_conf_options(*[make_remote_abspath(x) for x in config.spark_conf_options])
    cmd_builder.set_args(make_remote_abspath(config.spark_application_args))
    if config.spark_application_type == 'java':
        cmd_builder.set_class(config.spark_application_mainclass)
        cmd_builder.add_jars(*config.spark_extra_jars)
    command = cmd_builder.build()
    if _submit_blocking(config, command, interface.distribution['spark'], spark_master_id, spark_connectionwrappers=spark_connectionwrappers):
        prints('Experiment completed! (iteration {}/{})'.format(idx+1, num_experiments))
        return True
    else:
        printe('Fatal error for experiment iteration {}/{}'.format(idx+1, num_experiments))
        return False


def experiment_fetch_results_default(interface, idx, num_experiments, driver_node_id=None):
    '''Fetches results from the Spark node running the driver.
    Args:
        interface (ExperimentInterface): Experiment we are running right now.
        idx (int): Experiment index number. 0 for first experiment, 1 for seconds, etc.
        num_experiments (int): Amount of experiments we will run.
        driver_node_id (optional int): If set, skips searching for the driver node. Assumes node with given id is the driver instead.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config
    spark_master_id = interface.spark_master_id
    spark_nodes = interface.distribution['spark']

    if driver_node_id == None:
        if config.spark_deploymode == 'client': # We know the driver is executed on the spark master node in client mode.
            driver_node_id = spark_master_id
            driver_node = next(x for x in spark_nodes if x.node_id == driver_node_id)
            driver_connection_wrapper = _get_connection(config, driver_node)
        else: # We have to find the node that executes the driver in cluster mode.
            spark_connectionwrappers = _get_connections(config, spark_nodes)
            state, val = blocker.block_with_value(func_util.remote_file_find, args=(spark_connectionwrappers, remote_result_loc), return_val=True, sleeptime=10, dead_after_tries=3) 
            if state == blocker.BlockState.COMPLETE:
                driver_node_id = val[0]
                print('Found driver running on node_id={}'.format(driver_node_id))
            else:
                raise RuntimeError('Could not find results file on any node: {}'.format(remote_result_loc))
            driver_node, driver_connection_wrapper = next(x for x in spark_connectionwrappers.items() if node.node_id == driver_node_id)
    else:
        driver_node = next(x for x in spark_nodes if x.node_id == driver_node_id)
        driver_connection_wrapper = _get_connection(config, driver_node)

    fs.mkdir(config.result_dir, exist_ok=True)
    
    target_loc = fs.join(config.result_dir, config.result_file)
    if fs.isfile(target_loc):
        printw('Resultfile "{}" already exists, overwriting...'.format(target_loc))
        fs.rm(target_loc)
    return subprocess.call('rsync -e "ssh -F {}" -q -aHAX --inplace {}:{} {}'.format(driver_connection_wrapper.ssh_config.name, driver_node.ip_public, remote_result_loc, target_loc), shell=True) == 0



def register_default_experiment_function(interface, idx, num_experiments):
    interface.register('experiment_funcs', lambda iface: experiment_deploy_default(iface, idx, num_experiments))


def register_default_result_fetch_function(interface, idx, num_experiments):
    interface.register('result_fetch_funcs', lambda iface: experiment_fetch_results_default(iface, idx, num_experiments))