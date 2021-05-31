import concurrent.futures
from multiprocessing import cpu_count
import subprocess

import remoto

from experimenter.internal.remoto.util import get_ssh_connection as _get_ssh_connection
import utils.fs as fs

import experimenter.internal.data as data

from utils.printer import *



def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def deploy_data_default(interface, idx, num_experiments, nodes):
    config = interface.config
    path = config.data_path
    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if config.key_path:
            ssh_kwargs['IdentityFile'] = config.key_path
        else:
            printw('Connections have no assigned ssh key. Prepare to fill in your password often.')
        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=True, ssh_params=_merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']})) for x in nodes}
        connectionwrappers = {k: v.result() for k,v in futures_connection.items()}
        if any(x for x in connectionwrappers if not x):
            printe('Could not connect to some nodes.')
            return False
        if not (config.spark_silent or config.silent):
            print('Transferring data...')

        futures_mkdir = [executor.submit(remoto.process.check, x.connection, 'mkdir -p {}'.format(config.remote_data_dir), shell=True) for x in connectionwrappers.values()]
        if not all(x.result()[2] == 0 for x in futures_mkdir):
            printe('Could not create data destination directory for all nodes.')
            return False

        fun = lambda path, node, connectionwrapper: subprocess.call('rsync -e "ssh -F {}" -q -aHAX --inplace {} {}:{}'.format(connectionwrapper.ssh_config.name, path, node.ip_public, fs.join(config.remote_data_dir, fs.basename(path))), shell=True) == 0
        futures_rsync = [executor.submit(fun, path, node, connectionwrapper) for node, connectionwrapper in connectionwrappers.items()]
        if not all(x.result() for x in futures_rsync):
            printe('Could not connect to some nodes.')
            return False
        return True


def register_deploy_data(interface, idx, num_experiments, nodes=None):
    get_spark_nodes = lambda iface: nodes if nodes else interface.distribution['spark']

    interface.register('deploy_data_func', lambda iface: deploy_data_default(iface, idx, num_experiments, get_spark_nodes(iface)))




def generate_data_default(interface, idx, num_experiments):
    config = interface.config
    retval, num_rows = data.generate(config.data_generator_name, dest=config.data_path, stripe=config.stripe, num_columns=config.num_columns, data_format=config.data_format, extra_args=config.data_gen_extra_args, extra_kwargs=config.data_gen_extra_kwargs)
    if not retval:
        printe('Could not generate data using generator named "{}", destination: {} (iteration {}/{})'.format(config.data_generator_name, config.data_path, idx+1, num_experiments))
        return False 
    return True