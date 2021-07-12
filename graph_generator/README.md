# Graph Generation
Simple Graph creation tool.

Most of our experiments return a timeseries as datapoints consisting of 2 64-bit integers.
The first number is the initialization time Spark reader implementations needed to start up.
The second number is the computation time Spark needed.

To use the graph generator, use:
```bash
python3 graph_generator/entrypoint.py -h
```
By default, generated graphs are outputted to `/graph_generator/generated/`.



## Result Interpreter
Many results require an interpretation file.
An interpretation file is a Python script inside the results directory, used for all result files in its subdirectories.
For any result file `F`, we use the interpreter that is the nearest to `F`.
E.g, if the file structure is:
```
results/
    interpretA.py
    exp1/
        interpretB.py
        subdir/
            result1.res
            result2.res
            ...
    exp2/
        result3.res
```
Then, `result1.res` and `result2.res` get interpreted by `interpretB.py`, since that one is the closest (one directory up).
`result3.res` gets interpreted by `interpretA.py`.


### Adding Interpreters for your own data
When you have your own data, and want to have it interpreted:
 1. Create an Interpreter file
 2. (Optional) Place the Interpreter script in your resultset, such that your resultfiles will be interpreted by your script.

An interpreter file looks like:
```python
def filter(path):
    '''Determine if given path should be accepted or filtered out.
        Args:
            path (str): Full path to file to decide for.

        Returns:
            `True` to keep file in results, `False` to filter it out.'''
    return path.endswith('.res_a') or path.endswith('.res_s')

def to_identifiers(path):
    '''Transform given path to a number of identifiers.
    Args:
        path (str): Full path to file to build identifiers for.

    Returns:
        `dict(str, Any)`: Keyword identifiers.'''

    identifiers = dict()
    if path.endswith('.res_a'):
        identifiers['producer'] = 'arrow'
    elif path.endswith('.res_s'):
        identifiers['producer'] = 'spark'
    return identifiers


def sorting(frame):
    '''Sort result groups for grouped display.
    Args:
        frame (Frame): Frame to sort.

    Returns:
        `callable` sorting function to use when displaying results in a grouped manner.'''
    return frame.identifiers['producer']
```
Each of the functions can be left out, in which case the system searches the next-closest file to get the function.
If there is no function available in all interpret scripts, and when there are no scripts available for a path, uses default implementations defined by the used graph generator.

Each function can be called an arbitrary number of times.


### Creating your own graph generator
A graph generator receives a series of frames, and builds a graph.
We created a few graph generators inside [`/graph-generator/implementations/`](/graph-generator/implementations/).

A graph generator looks like:
```python
from graph_generator.interface import GeneratorInterface

def get_generator(*args, **kwargs):
    '''Implement this function in your generator. Make it return your generator class.'''
    raise SampleGraphBuilder(*args, **kwargs)

class SampleGraphBuilder(GeneratorInterface):
    '''Implement this interface to build a generator.'''


    def filter(self, path):
        '''Determine if given path should be accepted or filtered out.
        Args:
            path (str): Full path to file to decide for.

        Returns:
            `True` to keep file in results, `False` to filter it out.'''
        return path.endswith('.res_a') or path.endswith('.res_s') # Result files with our systems always have these extensions.


    def to_identifiers(self, path):
        '''Transform given path to a number of identifiers.
        Args:
            path (str): Full path to file to build identifiers for.

        Returns:
            `dict(str, Any)`: Keyword identifiers.'''
        raise NotImplementedError


    def sorting(self, frame):
        '''Sort result groups for grouped display.
        Args:
            frame (Frame): Frame to sort.

        Returns:
            `callable` sorting function to use when displaying results in a grouped manner.'''
        raise NotImplementedError


    def plot(self, frames, dest=None, show=True, large=False):
        '''Plot provided frames. Each frame has a number of identifiers, obtained from the `to_identifiers` function.
        Args:
            frames (iterable(Frame)): Frames with data to plot.
            dest (optional str): If set, stores generated graph to given destination, with given extension.
            show (optional bool): If set, shows generated graph.
            large (optional bool): If set, generates plots with large font.'''
        raise NotImplementedError
```
Interestingly, we see the same `filter`, `to_identifiers` and `sorting` functions as we saw in the interpret script structure.
The equivalently named functions in a graph generator script are used as a fallback:
When a result file has no interpret file in its path, or cannot find an interpret file with the right function, we call the functions here.
Note that the `filter`, `to_identifiers` and `sorting` functions in the graph generator are optional as well.
If left undefined, a `NotImplementedError` will be raised when there is need for a fallback function.

The plot function is the most interesting function of a graph generator.
It receives a series of frames, which contain the data, and several arguments to define what the generator should do:
 - If `dest` is set, should save the figure to set path.
 - If `show` is set, must show the generated figure on screen. Otherwise, should not show figure.
 - If `large` is set, should use extra large font for title, labels, ticks, legend etc.

What happens inside the `plot` function is up to the implementer.

#### Utilities
We provided a few helpful pieces of code with some standard functionality.
```python
import graph_generator.internal.util.storer as storer
storer.store_simple(dest, plt)
```
`storer` is a simple script that saves generated Matplotlib graphs to given destination.