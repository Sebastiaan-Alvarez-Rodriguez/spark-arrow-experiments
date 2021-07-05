import os
import re


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

    num_spark_nodes = int(os.path.basename(os.path.dirname(path)))
    identifiers['nodes'] = num_spark_nodes
    identifiers['group'] = str(identifiers['nodes'])
    return identifiers


def sorting(frame):
    '''Sort result groups for grouped display.
    Args:
        frame (Frame): Frame to sort.

    Returns:
        `callable` sorting function to use when displaying results in a grouped manner.'''
    return frame.identifiers['nodes']