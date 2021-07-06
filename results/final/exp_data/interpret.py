import os
import re


def _get_numbers(string):
    m = re.match(r'cp([0-9]+)_ln([0-9]+)', string)
    return int(m.group(1)), int(m.group(2))

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

    cp, ln = _get_numbers(os.path.basename(os.path.dirname(os.path.dirname(path))))
    identifiers['size'] = cp * ln
    identifiers['group'] = str(identifiers['size'])
    identifiers['group1'] = 'offload'
    return identifiers


def sorting(frame):
    '''Sort result groups for grouped display.
    Args:
        frame (Frame): Frame to sort.

    Returns:
        `callable` sorting function to use when displaying results in a grouped manner.'''
    return frame.identifiers['size']