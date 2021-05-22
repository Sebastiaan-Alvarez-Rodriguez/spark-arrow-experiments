import experimenter.internal.data as data

from utils.printer import *


def deploy_data_default(interface, config, idx, num_experiments):
    raise NotImplementedError


def generate_data_default(interface, idx, num_experiments):
    config = interface.config
    retval, num_rows = data.generate(config.data_generator_name, dest=config.data_path, stripe=config.stripe, num_columns=config.num_columns, data_format=config.data_format, extra_args=config.data_gen_extra_args, extra_kwargs=config.data_gen_extra_kwargs)
    if not retval:
        printe('Could not generate data using generator named "{}", destination: {} (iteration {}/{})'.format(config.data_generator_name, config.data_path, idx+1, num_experiments))
        return False 
    return True