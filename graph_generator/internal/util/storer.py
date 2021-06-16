import matplotlib.pyplot as plt

import utils.location as loc
import utils.fs as fs

def supported_filetypes():
    '''Returns an `iterable(str)`, containing the supported filetypes to store for. E.g. ('pdf', 'svg',...). '''
    generator = (x for x in plt.figure().canvas.get_supported_filetypes())
    plt.clf()
    plt.close()
    return generator

def filetype_is_supported(extension):
    '''Returns `True` iff matplotlib supports filetype, `False` otherwise.'''
    return str(extension).strip().lower() in supported_filetypes()

def store_simple(path, plotlike, **kwargs):
    fs.mkdir(loc.graph_generation_dir(), exist_ok=True)
    plotlike.savefig(fs.join(loc.graph_generation_dir(), path, **kwargs))

def store(dirname, filename, filetype, plotlike, **kwargs):
    # Stores given <plotlike> in dir <dirname>/<filename>.<filetype>, passing kwargs to <plotlike>.savefig()
    fs.mkdir(loc.graph_generation_dir(), dirname, exist_ok=True)
    plotlike.savefig(fs.join(loc.graph_generation_dir(), dirname, '{}.{}'.format(filename, filetype)), **kwargs)