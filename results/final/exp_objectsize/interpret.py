import os
import re


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

    objectsize = int(os.path.basename(os.path.dirname(path)))

    identifiers['group'] = objectsize
    identifiers['objectsize'] = objectsize
    return identifiers


def sorting(frame):
    '''Sort result groups for grouped display.
    Args:
        frame (Frame): Frame to sort.

    Returns:
        `callable` sorting function to use when displaying results in a grouped manner.'''     
    return (frame.identifiers['objectsize'])