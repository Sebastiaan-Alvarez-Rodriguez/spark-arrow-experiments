import os
import re


def _get_numbers(string):
    m = re.match(r'([0-9]+)_([0-9]+)', string)
    return int(m.group(1)), int(m.group(2))



def filter(path):
    return path.endswith('.res_a')

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

    objectsize, selectivity = _get_numbers(os.path.basename(os.path.dirname(path)))

    identifiers['group'] = selectivity
    identifiers['group1'] = str(objectsize)
    
    identifiers['selectivity'] = selectivity
    identifiers['objectsize'] = objectsize
    return identifiers


def sorting(frame):
    '''Sort result groups for grouped display.
    Args:
        frame (Frame): Frame to sort.

    Returns:
        `callable` sorting function to use when displaying results in a grouped manner.'''     
    return (frame.identifiers['selectivity'], frame.identifiers['objectsize'])