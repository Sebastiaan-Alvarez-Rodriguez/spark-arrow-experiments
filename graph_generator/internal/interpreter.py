from pathlib import Path
import threading

import utils.fs as fs
import utils.importer as importer
import utils.location as loc
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
        file_paths += list(x for x in fs.ls(cur, full_paths=True, only_files=True) if x.endswith('.py'))
    return file_paths


class Interpreter(object):
    '''Class to keep track of known result interpreters.
    Picks, loads, and applies functions from interpreters with greater shared path first.
    All `.py` files in any subdirectory of the given path are considered interpreters for that filetree.
    If multiple `.py` files exist in one subdirectory, visits in order of alphabet.
    Note: Implementation is thread-safe. Even when multiple threads are calling functions in parallel, no module is loaded twice, no undedfined states of this object can occur.'''

    def __init__(self, path, fallback_filter, fallback_to_identifiers, fallback_sorting, interpret_path=None, debug=False):
        '''Constructs a new Interpreter.
        Note: fallback_* parameters are only used when no interpret file could be found in any subdir from `path` to the filepath of a result file.
        Args:
            path (str): Path to search for interpret files.
            fallback_filter (callable): Callable with signature: bool func(str). Takes a str path, returns `True` if the path is to be filtered-in, `False` otherwise.
            fallback_to_identifiers (callable): Callable with signature: dict func(str). Takes a str path, returns a dict of string keys, Any vals that will be the identifiers for the file.
            fallback_sorting (callable): Callable with signature: Any func(optional str). Takes a str path, returns the Frame sorting to apply (e.g. lambda e: len(e) to sort on Frame length).
                                         rames contain the aforementioned identifiers, which could be used for sorting.
            interpret_path (optional str): If set, uses given file as interpret file. This file is always considered last.
            debug (optional bool): If set, prints more about.'''
        self.root_path = path #TODO: Should start finding interpret targets from folder named exp_.* if in path.
        
        self.interpret_targets = []#sorted([Path(x) for x in walk(self.root_path)], key=lambda e: (len(e.parts), e.parent))
        self.interpret_path = Path(interpret_path) if interpret_path != None else None


        self.fallback_filter = fallback_filter
        self.fallback_to_identifiers = fallback_to_identifiers
        self.fallback_sorting = fallback_sorting

        self.interpret_mapping = dict()
        self.lock = threading.Lock()
        self.debug = debug


    def get_or_insert_module(self, path):
        '''Gets or inserts module.
        Args:
            path (str, Path): Full path to import.

        Returns:
            Imported module.'''
        try:
            return self.interpret_mapping[path]
        except KeyError as e:
            self.lock.acquire()
            try:
                if path in self.interpret_mapping: # If `True`, the thread before us just loaded the right path.
                    return self.interpret_mapping[path]
                self.interpret_mapping[path] = importer.import_full_path(str(path))
                return self.interpret_mapping[path]
            finally:
                self.lock.release()


    def get_nearest_py(self, path):
        '''Finds the nearest interpret file, if it exists. Basically an 'onion walk'.
        Args:
            path (str, Path): Path to the resultfile.

        Returns:
            `Path` to nearest matching interpret file on success, `None` otherwise.'''
        p = Path(path) if isinstance(path, str) else path
        val = [x for x in self.interpret_targets if x.parent in p.parents]
        if self.interpret_path:
            val.append(self.interpret_path)
        return val


    def get_furthest_py(self, path):
        '''Much like `get_nearest_py(self, path)`, only fetches first matching path closest to root.'''
        p = Path(path) if isinstance(path, str) else path
        val = [x for x in self.interpret_targets[::-1] if p in x.parents]
        if self.interpret_path:
            val.append(self.interpret_path)
        return val



    def filter(self, path):
        '''Determine if given path should be accepted or filtered out.
        Note: If the result path contain a `.py` file, uses equivalent-named function from that file instead.
        Args:
            path (str): Full path to file to decide for.

        Returns:
            `True` to keep file in results, `False` to filter it out.'''
        for near in self.get_nearest_py(path):
            module = self.get_or_insert_module(near)
            try:
                if hasattr(module, 'filter'):
                    if self.debug:
                        print(f'filter serviced by file: {near}')
                    return module.filter(path)
            except Exception as e:
                printw(f'Module {near} experienced error: {e}')
        if self.debug:
            print(f'filter serviced by generator fallback.')
        return self.fallback_filter(path) # We have no more candidates. Use the fallback.


    def to_identifiers(self, path):
        '''Transform given path to a number of identifiers.
        Note: If the result path contain a `.py` file, uses equivalent-named function from that file instead.
        Args:
            path (str): Full path to file to build identifiers for.

        Returns:
            `dict(str, Any)`: Keyword identifiers.'''
        for near in self.get_nearest_py(path):
            module = self.get_or_insert_module(near)
            try:
                if hasattr(module, 'to_identifiers'):
                    if self.debug:
                        print(f'to_identifiers serviced by file: {near}')
                    return module.to_identifiers(path)
            except Exception as e:
                printw(f'Module {near} experienced error: {e}')
        if self.debug:
            print(f'to_identifiers serviced by generator fallback.')
        return self.fallback_to_identifiers(path) # We have no more candidates. Use the fallback.


    def sorting(self, path=None):
        '''Sort result groups for grouped display.
        Note: If the result path contain a `.py` file, uses equivalent-named function from that file instead.
        Returns:
            `callable` sorting function to use when displaying results in a grouped manner.'''
        if path == None:
            path = self.root_path
        for near in self.get_furthest_py(path):
            module = self.get_or_insert_module(near)
            try:
                if hasattr(module, 'sorting'):
                    if self.debug:
                        print(f'sorting serviced by file: {near}')
                    return module.sorting
                else:
                    printw(f'No filter module found for file: {near}')
            except Exception as e:
                printw(f'Module {near} experienced error: {e}')
        if self.debug:
            print(f'sorting serviced by generator fallback.')
        return self.fallback_sorting # We have no more candidates. Use the fallback.
