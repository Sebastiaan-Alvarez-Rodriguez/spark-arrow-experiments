import utils.fs as fs

def root():
    '''Returns absolute path to root of this project.'''
    return fs.dirname(fs.dirname(__file__))

def data_generator_dir():
    '''Returns directory where we store data generator implementations.'''
    return fs.join(root(), 'data_generator', 'implementations')

def data_generation_dir():
    '''Returns directory where we store generated parquet files.'''
    return fs.join(root(), 'data_generator', 'generated')

def graph_generator_dir():
    '''Returns directory where we store data generator implementations.'''
    return fs.join(root(), 'graph_generator', 'implementations')

def graph_generation_dir():
    '''Returns directory where we store generated parquet files.'''
    return fs.join(root(), 'graph_generator', 'generated')


def application_dir():
    '''Directory containing executables to deploy (or symlinks to them).'''
    return fs.join(root(), 'application')

def result_dir():
    '''Directory to contain results.'''
    return fs.join(root(), 'results')