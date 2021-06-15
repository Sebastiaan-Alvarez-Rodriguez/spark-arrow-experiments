import argparse

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Appends main project root as importpath.

import utils.fs as fs
import utils.location as loc
from utils.printer import *

import graph_generator.internal.generator as generator


'''Python CLI module to generate data.'''


def _default_stripe():
    return 4

def _default_generator():
    return sorted(fs.ls(loc.graph_generator_dir(), only_files=True))[0]


def add_args(parser):
    parser.add_argument('paths', nargs='+', help='Result path(s) to read from. Searches recursively for all files in given directory.')
    parser.add_argument('--generator', metavar='name', type=str, default=_default_generator(), help='Graph generator to execute (default={}).'.format(_default_generator()))
    parser.add_argument('--dest', type=str, help='If set, outputs plot to given output path. Point to a file, with an extension.')
    parser.add_argument('--no-show', dest='no_show', help='Do not show generated graph (useful on servers without xorg forwarding).', action='store_true')
    parser.add_argument('--large', help='If set, generates graphs with larger font.', action='store_true')
    parser.add_argument('--skip-leading', metavar='int', dest='skip_leading', type=int, default=0, help='If set, skips first n readings from every result frame. Supports negative numbers, which mean: Read the last abs(negative_number) values.')
    parser.add_argument('--extra-args', metavar='arg', dest='extra_args', type=str, nargs='+', default='', help='Extra args to pass to generator.')
    parser.add_argument('--extra-kwargs', metavar='kwarg', dest='extra_kwargs', type=str, nargs='+', default='', help='Extra kwargs to pass to generator.')

def main():
    parser = argparse.ArgumentParser(
        prog='generator',
        formatter_class=argparse.RawTextHelpFormatter,
        description='Generate graphs from results.'
    )
    retval = True
    add_args(parser)

    args = parser.parse_args()
    extra_args = list(args.extra_args.split())
    extra_kwargs = {x.split('=') for x in args.extra_kwargs.split()}
    retval = generator.generate(args.generator, args.paths, dest=args.dest, show=not args.no_show, large=args.large, skip_leading=args.skip_leading, args=extra_args, kwargs=extra_kwargs)[0]

    if isinstance(retval, bool):
        exit(0 if retval else 1)
    elif isinstance(retval, int):
        exit(retval)
    else:
        exit(0 if retval else 1)


if __name__ == '__main__':
    main()