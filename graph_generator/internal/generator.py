import itertools

import utils.fs as fs
import utils.importer as importer
import utils.location as loc
from utils.printer import *

from graph_generator.interface import GeneratorInterface
from graph_generator.internal.util.reader import read
from graph_generator.internal.interpreter import Interpreter

def _import_module(generator_name):
    if (not fs.isfile(loc.graph_generator_dir(), generator_name)) and not generator_name.endswith('.py'):
        generator_name = generator_name+'.py'
    if not fs.isfile(loc.graph_generator_dir(), generator_name):
        return None
    return importer.import_full_path(fs.join(loc.graph_generator_dir(), generator_name))


def generate(generator_name, paths, interpret_path=None, dest=None, show=True, large=False, skip_leading=0, args=None, kwargs=None):
    '''Generates requested `data_format`, using requested `generator_name`.
    Args:
        generator_name (str): Name of generator. Must be present in `data_generator/implementations/`. A `.py` extension does not have to be specified.
        paths (iterable(str)): Paths to directories to read files from.
        interpret_path (optional str): If set, uses given file as interpret file. This file is always considered last.
        dest (optional str): If set, stores generated graph to given output path.
        show (optional bool): If set, shows graph. Otherwise, does not show anything.
        large (optional bool): If set, generates graph with larger font.
        skip_leading (optional int): If set, skips first n readings from every result frame. Supports negative numbers, which mean: Read the last abs(negative_number) values.
        args (optional list(str)): Extra arguments to pass to generator function.
        kwargs (optional dict(str, str): Extra keyword arguments to pass to generator function.'''

    module = _import_module(generator_name)
    if not module:
        printe('Generator "{}" not found at: {}'.format(generator_name, fs.join(loc.graph_generator_dir(), generator_name)))
        return False, None

    generator = module.get_generator(*args, **kwargs if kwargs else {})
    if not GeneratorInterface.is_graph_generator(generator):
        printe('Generator "{}" is no graph generator.'.format(generator.__class__.__name__))
        return False, None

    fs.mkdir(fs.dirname(loc.graph_generation_dir()), exist_ok=True)

    frames = []
    for path in paths:
        interpreter = Interpreter(path, generator.filter, generator.to_identifiers, generator.sorting, interpret_path=interpret_path, debug=True)
        frames.append(read(path, interpreter, skip_leading=skip_leading))
    outputgraph_path = generator.plot(itertools.chain(*frames), dest=dest, show=show, large=large)

    return True, (outputgraph_path if dest else None)