import abc

def get_generator(*args, **kwargs):
    '''Implement this function in your generator. Make it return your generator class.'''
    raise NotImplementedError

class GeneratorInterface(metaclass=abc.ABCMeta):
    '''Implement this interface to build a generator.'''


    class_id = 6136638678287157381

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


    @staticmethod
    def is_graph_generator(obj):
        try:
            return obj.class_id == GeneratorInterface.class_id
        except AttributeError as e:
            return False