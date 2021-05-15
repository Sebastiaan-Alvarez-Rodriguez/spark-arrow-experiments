import argparse

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Appends main project root as importpath.

import utils.fs as fs
import utils.location as loc
import utils.importer as importer
from utils.printer import *

from data_generator.internal.data_format import DataFormat
import data_generator.internal.generator as generator


'''Python CLI module to generate data.'''


def _default_stripe():
    return 4

def _default_generator():
    return sorted(fs.ls(loc.data_generator_dir(), only_files=True))[0]


def add_args(parser):
    parser.add_argument('dest', type=str, help='Generator output path (including file).')
    parser.add_argument('format', type=str, default='parquet', help='Data format to generate. One of: {}'.format(', '.join(x.name for x in DataFormat)))
    parser.add_argument('--generator', type=str, default=_default_generator(), help='Data generator to execute (default={}).'.format(_default_generator()))
    parser.add_argument('--stripe', metavar='amount', type=int, default=_default_stripe(), help='Striping, in megabytes (default={}MB). Must be a multiple of 4. Every file has to be smaller than stripe size.'.format(_default_stripe()))
    parser.add_argument('--num-columns', dest='num_columns', type=int, default=4, help='Number of columns to generate (default=4).')
    parser.add_argument('--extra-args', dest='extra_args', type=str, nargs='+', default='', help='Extra args to pass to generator.')
    parser.add_argument('--extra-kwargs', dest='extra_kwargs', type=str, nargs='+', default='', help='Extra kwargs to pass to generator.')

def main():
    parser = argparse.ArgumentParser(
        prog='generator',
        formatter_class=argparse.RawTextHelpFormatter,
        description='Generate data for experiments.'
    )
    retval = True
    add_args(parser)

    args = parser.parse_args()

    extra_args = list(args.extra_args.split())
    extra_kwargs = {x.split('=') for x in args.extra_kwargs.split()}
    retval = generator.generate(args.generator, args.dest, args.stripe, args.num_columns, args.format, extra_args, extra_kwargs)[0]

    if isinstance(retval, bool):
        exit(0 if retval else 1)
    elif isinstance(retval, int):
        exit(retval)
    else:
        exit(0 if retval else 1)


if __name__ == '__main__':
    main()