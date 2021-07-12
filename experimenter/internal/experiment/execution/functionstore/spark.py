import metareserve
import spark_deploy

from utils.printer import *

def install_spark(interface, idx, num_experiments):
    '''Installs Spark.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.

    Required distributions:
        'spark'

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        spark_download_url (str or None): URL to Spark distribution to install.
        spark_force_reinstall (bool): If set, reinstalls Spark, even when a valid installation is present.
        spark_silent (bool): Indication whether Spark output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config
    if not spark_deploy.install(metareserve.Reservation(interface.distribution['spark']), key_path=config.key_path, spark_url=config.spark_download_url, force_reinstall=config.spark_force_reinstall, silent=config.spark_silent or config.silent):
        printe('Could not install Spark (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    return True


def start_spark(interface, idx, num_experiments):
    '''Starts Spark.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.

    Required distributions:
        'spark'

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        spark_workdir (str or None): Remote path that Spark workers use as workdir.
        spark_start_stop_with_sudo (bool): If set, starts Spark as sudo user. Warning: Spark does not like being started as sudo.
        spark_silent (bool): Indication whether Spark output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config
    retval, spark_master_id, spark_master_url = spark_deploy.start(metareserve.Reservation(interface.distribution['spark']), key_path=config.key_path, worker_workdir=config.spark_workdir, use_sudo=config.spark_start_stop_with_sudo, silent=config.spark_silent or config.silent)
    if not retval:
        printe('Could not start Spark (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    setattr(interface, 'spark_master_id', spark_master_id)
    setattr(interface, 'spark_master_url', spark_master_url)
    return True


def stop_spark(interface, idx, num_experiments):
    '''Stops Spark.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.

    Required distributions:
        'spark'

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        spark_workdir (str or None): Remote path that Spark workers use as workdir.
        spark_start_stop_with_sudo (bool): If set, starts Spark as sudo user. Warning: Spark does not like being started as sudo.
        spark_silent (bool): Indication whether Spark output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config
    if not spark_deploy.stop(metareserve.Reservation(interface.distribution['spark']), key_path=config.key_path, worker_workdir=config.spark_workdir, use_sudo=config.spark_start_stop_with_sudo, silent=config.spark_silent or config.silent):
        printe('Could not stop Spark (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    return True


def register_spark_functions(interface, idx, num_experiments):
    '''Registers required Spark functions.'''
    interface.register('install_spark_func', lambda iface: install_spark(iface, idx, num_experiments))
    interface.register('start_spark_func', lambda iface: start_spark(iface, idx, num_experiments))
    interface.register('stop_spark_func', lambda iface: stop_spark(iface, idx, num_experiments))