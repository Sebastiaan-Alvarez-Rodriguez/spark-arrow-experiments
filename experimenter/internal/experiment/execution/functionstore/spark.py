import metareserve
import spark_deploy

from utils.printer import *

def install_spark(interface, idx, num_experiments):
    config = interface.config
    if not spark_deploy.install(metareserve.Reservation(interface.distribution['spark']), key_path=config.key_path, spark_url=config.spark_download_url, force_reinstall=config.spark_force_reinstall, silent=config.spark_silent or config.silent):
        printe('Could not install Spark (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    return True


def start_spark(interface, idx, num_experiments):
    config = interface.config
    retval, spark_master_id, spark_master_url = spark_deploy.start(metareserve.Reservation(interface.distribution['spark']), key_path=config.key_path, worker_workdir=config.spark_workdir, use_sudo=config.spark_start_stop_with_sudo, silent=config.spark_silent or config.silent)
    if not retval:
        printe('Could not start Spark (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    setattr(interface, 'spark_master_id', spark_master_id)
    setattr(interface, 'spark_master_url', spark_master_url)
    return True


def stop_spark(interface, idx, num_experiments):
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