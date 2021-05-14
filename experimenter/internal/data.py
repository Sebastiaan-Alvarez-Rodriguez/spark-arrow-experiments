from data_generator.internal.generator import generate as _data_generate
import utils.location as loc

# generator_name, dest, stripe, num_columns, data_format, extra_args=None, extra_kwargs=None
def generate(data_generator_name, dest=loc.data_generation_dir(), stripe=64, num_columns=4, data_format='parquet', extra_args=None, extra_kwargs=None):
    '''Forwarding function to generate data.
    Args:
        data_generator_name (str): Name of the data generator to execute (must be available in /data_generator/implementations).
        dest (optional str): Path to output destination.
        stripe (optional int): Stripe size in MB to adhere to (generated output must be less than given size).
        num_columns (optional int): Number of columns to generate.
        data_format (optinal str): Data format to generate.

    Returns:
        `(True, path_to_file)` on success, `(False, None)` on failure.'''
    return _data_generate(data_generator_name, dest, stripe, num_columns, data_format, extra_args=extra_args, extra_kwargs=extra_kwargs)