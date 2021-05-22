import spark_deploy
import experimenter.internal.experiment.blocker as blocker

import utils.fs as fs
from utils.printer import *

def _submit_blocking(config, command, spark_connectionwrappers, spark_master_id):
    '''Submits Spark command. Waits on completion by checking the amount of results gathered to this point.
    If the system appears to have crashed, we reboot it and make it continue.
    Args:
        config (ExperimentConfiguration): Configuration to read control parameters from.
        command (str): Command to provide to spark-submit.
        spark_connectionwrappers (dict(metareserve.Node, RemotoSSHWrapper): Dict mapping all Spark nodes to connections.
        spark_master_id (int): Node id of the Spark master node.

    Returns:
        `True` if the run is complete and we collected enough data. `False` if the run crashed too many times.'''
    results_file = fs.join(config.resultloc, config.resultfile)
    lines_needed = config.runs

    for _try in range(config.tries):
        if not spark_deploy.submit(metareserve.Reservation([x for x in spark_connectionwrappers.keys()]), command, paths=config.local_application_paths, key_path=config.key_path, master_id=spark_master_id, use_sudo=config.spark_submit_with_sudo, silent=config.spark_silent or config.silent):
            printw('Could not submit application on remote. Used command: {}'.format(command))
            return False

        if config.spark_deploymode == 'client': # We know the driver is executed on the spark master node in client mode.
            driver_node_id = spark_master_id
        else: # We have to find the node that executes the driver in cluster mode.
            state, val = blocker.block_with_value(func_util.remote_file_find, args=(spark_connectionwrappers, results_file), sleeptime=10, dead_after_tries=3) 
            if state == blocker.BlockState.COMPLETE:
                driver_node_id = val
            else:
                raise RuntimeError('Could not find results file on any node: {}'.format(results_file))

        driver_node = next(node for node, wrapper in spark_connectionwrappers.items() if node.node_id == driver_node_id)
        state, val = blocker.block_with_value(func_util.remote_count_lines, args=(spark_connectionwrappers[driver_node].connection, results_file, lines_needed), sleeptime=config.sleeptime, dead_after_tries=config.dead_after_tries)
        if state == blocker.BlockState.COMPLETE:
            return True
        if state == blocker.BlockState.TIMEOUT:
            printw('System timeout detected. Current status: {}/{}'.format(val, config.runs))
            lines_needed += 1 # +1 because we need a new line for warming caches.
    return False


def experiment_deploy_default(interface, idx, num_experiments):
    config = interface.config
    spark_master_id = interface.spark_master_id
    spark_master_url = interface.spark_master_url
    # spark_connectionwrappers
    raise NotImplementedError

    cmd_builder = spark_deploy.SubmitCommandBuilder(cmd_type=config.spark_application_type)
    cmd_builder.set_master(spark_master_url)
    cmd_builder.set_deploymode(config.spark_deploymode)
    cmd_builder.add_java_options(*config.spark_java_options)
    cmd_builder.set_application(config.spark_application_path)
    cmd_builder.add_conf_options(*config.spark_conf_options)
    cmd_builder.set_args(config.spark_application_args)
    if config.spark_application_type == 'java':
        cmd_builder.set_class(config.spark_application_mainclass)
        cmd_builder.add_jars(*config.spark_extra_jars)
    command = cmd_builder.build()
    if _submit_blocking(config, command, spark_connectionwrappers, spark_master_id):
        prints('Experiment completed! (iteration {}/{})'.format(idx+1, num_experiments))
        return True
    else:
        printe('Fatal error for experiment iteration {}/{}'.format(idx+1, num_experiments))
        return False


def register_default_experiment_function(interface, idx, num_experiments):
    interface.register('experiment_funcs', lambda iface: experiment_deploy_default(iface, idx, num_experiments))