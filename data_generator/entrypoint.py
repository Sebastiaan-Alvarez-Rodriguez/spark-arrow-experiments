import argparse

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__)))) # Appends main project root as importpath.


import utils.fs as fs
import utils.importer as importer
from utils.printer import *


'''Python CLI module to deploy RADOS-Ceph on metareserve-allocated resources.'''

def generators_dir():
    return fs.join(fs.abspath(fs.dirname(__file__)), 'implementations')

def output_dir():
    return fs.join(fs.abspath(fs.dirname(__file__)), 'generated')


def _default_stripe():
    return 4

def _default_generator():
    return sorted(fs.ls(generators_dir(), only_files=True))[0]



def generate(gen, dest, stripe):
    if (not fs.isfile(generators_dir(), gen)) and not gen.endswith('.py'):
        gen = gen+'.py'
    if not fs.isfile(generators_dir(), gen):
        printe('Generator "{}" not found at: {}'.format(gen, fs.join(generators_dir(), gen)))
        return False, None

    fs.mkdir(dest, exist_ok=True)
    module = importer.import_full_path(fs.join(generators_dir(), gen))
    print('Generating using "{}", stripe size {}MB...'.format(gen, stripe))
    retval, path = module.generate(dest, stripe)
    if retval:
        gen_size = os.path.getsize(path)
        if gen_size > stripe*1024*1024:
            printe('Generated output is too large! Found size: {} ({:.02f}MB) > {} ({}MB)'.format(gen_size, round(gen_size/1024/1024, 2), stripe*1024*1024, stripe))
        else:
            prints('Generated output ready: Written size: {} ({:.02f}MB) <= {} ({}MB)'.format(gen_size, round(gen_size/1024/1024, 2), stripe*1024*1024, stripe))
    return retval, path


def add_args(parser):
    parser.add_argument('--gen', type=str, default=_default_generator(), help='Data generator to execute (default={})'.format(_default_generator()))
    parser.add_argument('--dest', type=str, default=output_dir(), help='Generator output dir (default={})'.format(output_dir()))
    parser.add_argument('--stripe', metavar='amount', type=int, default=_default_stripe(), help='Striping, in megabytes (default={}MB). Must be a multiple of 4. Every file has to be smaller than set stripe size.'.format(_default_stripe()))


def main():
    parser = argparse.ArgumentParser(
        prog='generator',
        formatter_class=argparse.RawTextHelpFormatter,
        description='Generate data for experiments.'
    )
    retval = True
    add_args(parser)

    args = parser.parse_args()

    retval = generate(args.gen, args.dest, args.stripe)[0]

    if isinstance(retval, bool):
        exit(0 if retval else 1)
    elif isinstance(retval, int):
        exit(retval)
    else:
        exit(0 if retval else 1)


if __name__ == '__main__':
    main()