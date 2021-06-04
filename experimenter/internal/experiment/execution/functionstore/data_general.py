import data_deploy
import metareserve

import experimenter.internal.data as data

from utils.printer import *



def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def deploy_data_default(interface, idx, num_experiments, nodes, *args, plugin='star_remote', **kwargs):
    config = interface.config
    return data_deploy.deploy(metareserve.Reservation(nodes), *args, key_path=config.key_path, paths=[config.data_path], dest=config.remote_data_dir, copy_multiplier=config.copy_multiplier, link_multiplier=config.link_multiplier, silent=config.ceph_silent or config.silent, plugin=plugin, **kwargs)


def register_deploy_data(interface, idx, num_experiments, nodes=None, plugin='star_remote'):
    get_spark_nodes = lambda iface: nodes if nodes else iface.distribution['spark']
    interface.register('deploy_data_func', lambda iface: deploy_data_default(iface, idx, num_experiments, get_spark_nodes(iface), plugin=plugin))


def generate_data_default(interface, idx, num_experiments):
    config = interface.config
    retval, num_rows = data.generate(config.data_generator_name, dest=config.data_path, stripe=config.stripe, num_columns=config.num_columns, data_format=config.data_format, extra_args=config.data_gen_extra_args, extra_kwargs=config.data_gen_extra_kwargs)
    if not retval:
        printe('Could not generate data using generator named "{}", destination: {} (iteration {}/{})'.format(config.data_generator_name, config.data_path, idx+1, num_experiments))
        return False 
    return True