from data_generator.entrypoint import generate as _data_generate

def data_generate(data_generator_name, dest, stripe):
    '''Forwarding function to generate data.
    Args:
        data_generator_name (str): Name of the data generator to execute (must be available in /data_generator/implementations).
        dest (str): Path to output destination.
        stripe (int): Stripe size in MB to adhere to (generated output must be less than given size).

    Returns:
        `True` on success, `False` on failure.'''
    return _data_generate(data_generator_name, dest, stripe)