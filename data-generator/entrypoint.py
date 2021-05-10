import argparse

import os
import sys

import internal.util.fs as fs
import internal.util.importer as importer
from internal.util.printer import *

'''Python CLI module to deploy RADOS-Ceph on metareserve-allocated resources.'''

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__)))) # Appends main project root as importpath.

def generators_dir():
    return fs.join(fs.abspath(), 'implementations')

def output_dir():
    return fs.join(fs.abspath(), 'generated')


def _default_stripe():
    return 4

def _default_generator():
    return sorted(fs.ls(generators_dir(), only_files=True))[0]



def add_args(parser):
    parser.add_argument('--gen', type=str, default=_default_generator(), help='Data generator to execute (default={})'.format(_default_generator()))
    parser.add_argument('--dest', type=str, default=output_dir(), help='Generator output dir (default={})'.format(output_dir()))
    parser.add_argument('--stripe', metavar='amount', type=int, default=_default_stripe(), help='Striping, in megabytes (default={}MB). Must be a multiple of 4. Every file has to be smaller than set stripe size.'.format(_default_stripe()))


def main():
    parser = argparse.ArgumentParser(
        prog='generator',
        formatter_class=argparse.RawTextHelpFormatter,
        description='Deploy RADOS-ceph on clusters'
    )
    retval = True
    add_args(parser)

    args = parser.parse_args()

    if fs.isfile(generators_dir(), args.gen):
        fs.mkdir(args.dest, exist_ok=True)
        module = importer.import_full_path(fs.join(generators_dir(), args.gen))
        print('Generating using "{}", stripe size {}MB...'.format(args.gen, args.stripe))
        retval, path = module.generate(args.dest, args.stripe)
        if retval:
            gen_size = os.path.getsize(path)
            if gen_size > args.stripe*1024*1024:
                printe('Generated output is too large! Found size: {} ({:.02f}MB) > {} ({}MB)'.format(gen_size, round(gen_size/1024/1024, 2), args.stripe*1024*1024, args.stripe))
                retval = False
            else:
                prints('Generated output ready: Written size: {} ({:.02f}MB) <= {} ({}MB)'.format(gen_size, round(gen_size/1024/1024, 2), args.stripe*1024*1024, args.stripe))
    else:
        printe('Generator "{}" not found at: {}'.format(args.gen, fs.join(generators_dir(), args.gen)))
        mainparser.print_help()
        retval = False

    if isinstance(retval, bool):
        exit(0 if retval else 1)
    elif isinstance(retval, int):
        exit(retval)
    else:
        exit(0 if retval else 1)


if __name__ == '__main__':
    main()