import data_deploy
import metareserve

import experimenter.internal.data as data

from utils.printer import *



def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def deploy_data_default(interface, idx, num_experiments, nodes, *args, plugin='star_remote', **kwargs):
    '''Uses the data-deploy package to get data to a series of nodes.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.
        nodes (list(metareserve.Nodes)): Nodes to deploy data for.
        *args: Other arguments for data-deploy, deploy function.
        plugin (optional str): data-deploy plugin name to use.
        **kwargs: Other keyword arguments for data-deploy, deploy function.

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        data_path (str): Path to data to transmit.
        remote_data_dir (str): Data destination directory on remote.
        copy_multiplier (int): Amount of copies of each file to make on the remote.
        link_multiplier (int): Amount of hardlinks of each file to make on the remote.
        ceph_silent (bool): Indication whether Ceph output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config
    return data_deploy.deploy(metareserve.Reservation(nodes), *args, key_path=config.key_path, paths=[config.data_path], dest=config.remote_data_dir, copy_multiplier=config.copy_multiplier, link_multiplier=config.link_multiplier, silent=config.ceph_silent or config.silent, plugin=plugin, **kwargs)



def generate_data_default(interface, idx, num_experiments, *args, plugin='num_generator', **kwargs):
    '''Uses the data_generator subproject to generate testdata.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.
        *args: Other arguments for data_generator.
        plugin (optional str): data_generator plugin name to use.
        **kwargs: Other keyword arguments for data_generator.

    Required config args:
        data_path (str): Path to data to generate to.
        stripe (int): Target output filesize, in MB. Never generates more than this many MB.
        num_columns (int): Amount of columns to generate.
        data_format (str): Format to generate.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config
    retval, num_rows = data.generate(plugin, dest=config.data_path, stripe=config.stripe, num_columns=config.num_columns, data_format=config.data_format, extra_args=args, extra_kwargs=kwargs)
    if not retval:
        printe('Could not generate data using generator named "{}", destination: {} (iteration {}/{})'.format(plugin, config.data_path, idx+1, num_experiments))
        return False 
    return True



def register_deploy_data(interface, idx, num_experiments, *args, nodes=None, plugin='star_remote', **kwargs):
    '''Uses the data-deploy package to get data to a series of nodes.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.
        nodes (list(metareserve.Nodes)): Nodes to deploy data for.
        *args: Other arguments for data-deploy, deploy function.
        plugin (optional str): data-deploy plugin name to use.
        **kwargs: Other keyword arguments for data-deploy, deploy function.

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        data_path (str): Path to data to transmit.
        remote_data_dir (str): Data destination directory on remote.
        copy_multiplier (int): Amount of copies of each file to make on the remote.
        link_multiplier (int): Amount of hardlinks of each file to make on the remote.
        ceph_silent (bool): Indication whether Ceph output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.'''
    get_spark_nodes = lambda iface: nodes if nodes else iface.distribution['spark']
    interface.register('deploy_data_func', lambda iface: deploy_data_default(iface, idx, num_experiments, get_spark_nodes(iface), *args, plugin=plugin, **kwargs))