from data_generator.entrypoint import generate as _data_generate
import utils.location as loc

def generate(data_generator_name, dest=loc.data_generation_dir(), stripe=64):
    '''Forwarding function to generate data.
    Args:
        data_generator_name (str): Name of the data generator to execute (must be available in /data_generator/implementations).
        dest (optional str): Path to output destination.
        stripe (optional int): Stripe size in MB to adhere to (generated output must be less than given size).

    Returns:
        `(True, path_to_file)` on success, `(False, None)` on failure.'''
    return _data_generate(data_generator_name, dest, stripe)