import os

import utils.fs as fs
import utils.location as loc
import utils.importer as importer
from utils.printer import *

from data_generator.internal.data_format import DataFormat

def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def _import_module(generator_name):
    if (not fs.isfile(loc.data_generator_dir(), generator_name)) and not generator_name.endswith('.py'):
        generator_name = generator_name+'.py'
    if not fs.isfile(loc.data_generator_dir(), generator_name):
        return None
    return importer.import_full_path(fs.join(loc.data_generator_dir(), generator_name))


def generate(generator_name, dest, stripe, num_columns, data_format, extra_args=None, extra_kwargs=None):
    '''Generates requested `data_format`, using requested `generator_name`.
    Args:
        generator_name (str): Name of generator. Must be present in `data_generator/implementations/`. A `.py` extension does not have to be specified.
        dest (str): Path to output file. Non-existing directories in the path will be created, if possible.
        stripe (int): Stripe size to use, in megabytes. Files generated must be smaller than this size.
        num_columns (int): Number of columns to generate.
        data_format (str): DataFormat to use. See `data_generator.internal.data_format.DataFormat` for options.
        extra_args (optional list(str)): Extra arguments to pass to generator function.
        extra_kwargs (optional dict(str, str): Extra keyword arguments to pass to generator function.'''
    dest = fs.abspath(dest)
    if fs.isdir(dest):
        printe('Given generation location is a directory (need path to (potentially non-existing) file): {}'.format(dest))
        return False, None
    data_format = DataFormat.from_string(data_format)

    module = _import_module(generator_name)
    if not module:
        printe('Generator "{}" not found at: {}'.format(generator_name, fs.join(loc.data_generator_dir(), generator_name)))
        return False, None

    fs.mkdir(fs.dirname(dest), exist_ok=True)

    registry = module.register()
    if not data_format in registry:
        printe('Module "{}" has no support for data format {}'.format(generator_name, data_format.to_string()))
        return False, None

    # stripe, num_columns=4, scheme_amount_bytes=int(1.5*1024*1024), compression=Compression.NONE
    print('Generating using "{}", stripe size {}MB...'.format(generator_name, stripe))
    
    args = [dest, stripe, num_columns]
    kwargs = dict()
    if data_format == DataFormat.PARQUET:
        pass # No extra (kw)args to pass
    elif data_format == DataFormat.CSV:
        pass # No extra (kw)args to pass

    args += extra_args if extra_args else []
    if extra_kwargs:
        kwargs = _merge_kwargs(kwargs, extra_kwargs)

    retval, num_rows = registry[data_format](*args, **kwargs)
    if retval:
        gen_size = os.path.getsize(dest)
        if gen_size > stripe*1024*1024:
            printe('Generated output is too large! Found size: {} ({:.02f}MB) > {} ({}MB)'.format(gen_size, round(gen_size/1024/1024, 2), stripe*1024*1024, stripe))
        else:
            prints('Generated output ready: Written size: {} ({:.02f}MB) <= {} ({}MB)'.format(gen_size, round(gen_size/1024/1024, 2), stripe*1024*1024, stripe))
    return retval, num_rows