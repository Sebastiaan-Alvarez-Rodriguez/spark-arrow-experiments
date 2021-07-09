import concurrent.futures
from multiprocessing import cpu_count

import numpy as np

import utils.fs as fs
from utils.printer import *


def walk(path):
    '''Performs a depth-first search walk over the filesystem. We assume no recursion through symlinks is possible, and that all files are relevant.
    Args:
        path (str): Full path to search.

    Returns:
        `list(str)` containing full paths to all files.'''
    paths = [path]
    file_paths = []
    while any(paths):
        cur = paths.pop() # pop takes last element from list.
        paths += list(fs.ls(cur, full_paths=True, only_dirs=True)) # Appends paths to end of list.
        file_paths += list(fs.ls(cur, full_paths=True, only_files=True))
    return file_paths


def read(paths, interpreter, skip_leading=0):
    '''For every path, searches all contained files (including subdirectories). Filters files. Kept files are read into a `Frame` and sent back as an iterable.
    1. Given a path, needs to find all files in all subdirectories.
    2. Needs to accept a lambda function to turn a path to a dict of identifiers.
    3. Needs to return frames for paths with identifiers.
    It is the responsibility of the generators to provide decent identifier functions.
    It is the responsibility of the generators to decide how stuff should be plotted with those identifiers.
    Args:
        paths (str,iterable(str)): Path or paths to search for files.
        interpreter (Interpreter): Interpreter instance to provide `filter`, `to_identifiers` and `sorting` functionality.
        skip_leading (optional int): If set, skips reading the set number of lines. Supports negative numbers, which mean: Read the last abs(negative_number) values.

    Returns:
        `iterable(Frame)`: An iterable of frames containing the data from a file.'''
    if isinstance(paths, str):
        paths = [paths]
    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
        futures_walk = [executor.submit(walk, path) for path in paths]
        paths = []
        for x in futures_walk:
            paths += x.result()
        num_total_files = len(paths)
        print('Found {} files.'.format(num_total_files))
        futures_filter = [executor.submit(interpreter.filter, x) for x in paths]
        accepted_paths = []
        for idx, x in enumerate(futures_filter):
            if x.result():
                accepted_paths.append(paths[idx])
        del paths
    print('Accepted {}/{} files.'.format(len(accepted_paths), num_total_files))
    return (Frame.from_file(x, sort_func=interpreter.sorting(), skip_leading=skip_leading, **interpreter.to_identifiers(x)) for x in accepted_paths)


class Frame(object):
    '''Frames hold data in numpy arrays, with identifiers.'''

    '''Creates a new frame.
    Args:
        lines (list(str)): Read datalines. Assumes no trailing '\n'.
        skip_leading (optional int): If set, skips reading the set number of lines. Supports negative numbers, which mean: Read the last abs(negative_number) values.
        **kwargs: All other kwargs are assumed to be identifiers for this Frame. E.g. if the data was obtained with a framework named X, then `framework='X'` could be specified as identifier. 

    Returns:
    '''
    def __init__(self, lines, skip_leading=0, sort_func=lambda e: e, **kwargs):
        lines = lines[skip_leading:]
        self.i_arr = np.array([int(x.split(',', 1)[0]) for idx, x in enumerate(lines)])
        self.c_arr = np.array([int(x.split(',', 1)[1]) for idx, x in enumerate(lines)])
        self.sort_func = sort_func
        self._identifiers = dict(kwargs)

    @staticmethod
    def from_file(path, skip_leading=0, sort_func=lambda e: 0, **kwargs):
        with open(path, 'r') as f:
            lines = f.readlines()
        return Frame(lines, skip_leading=skip_leading, sort_func=sort_func, **kwargs)

    @property
    def size(self):
        return len(self.i_arr)

    @property
    def identifiers(self):
        return self._identifiers

    @property
    def empty(self):
        return not any(self.i_arr)
    
    @property
    def i_time(self):
        return float(np.sum(self.i_arr)) / 1000000000
    
    @property
    def c_time(self):
        return float(np.sum(self.c_arr)) / 1000000000
    
    @property
    def total_time(self):
        return self.i_time+self.c_time
    
    @property
    def i_avgtime(self):
        return np.average(self.i_arr) / 1000000000
        
    @property
    def c_avgtime(self):
        return np.average(self.c_arr) / 1000000000
    
    @property
    def total_avgtime(self):
        return self.i_avgtime+self.c_avgtime    

    def __str__(self):
        if 'label' in self.identifiers:
            return self.identifiers['label']
        local_identifiers = dict(self.identifiers)
        del local_identifiers['group']
        return ', '.join('{}:{}'.format(k, v) for k,v in self.identifiers.items())

    def __repr__(self):
        return ', '.join('{}:{}'.format(k, v) for k,v in self.identifiers.items())

    def __len__(self):
        return self.size