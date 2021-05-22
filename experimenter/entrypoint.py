import argparse

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Appends main project root as importpath.

import utils.fs as fs
import utils.importer as importer
from utils.printer import *

from experimenter.internal.reservation import read_reservation_cli

import experimenter.internal.experiment.executor as executor


'''Python CLI module to deploy RADOS-Ceph on metareserve-allocated resources.'''


def experiments_dir():
    return fs.join(fs.abspath(), 'implementations')

def output_dir():
    return fs.join(fs.abspath(), 'generated')


def _default_stripe():
    return 4


def _load_experiment(name):
    module = importer.import_full_path(fs.join(experiments_dir(), name))
    return module.get_experiment()


def experiment(names):
    for idx, name in enumerate(names):
        if not fs.isfile(experiments_dir(), name) and not name.endswith('.py'):
            names[idx] = name+'.py'
        if not fs.isfile(experiments_dir(), name):
            printe('Experiment "{}" not found at: {}'.format(name, fs.join(experiments_dir(), name)))
            return False

    experiment_mapping = {name: _load_experiment(name) for name in names}

    reservation = read_reservation_cli()
    if not reservation:
        return False
    return executor.execute(experiment_mapping, reservation)


def add_args(parser):
    parser.add_argument('experiments', metavar='name', nargs='+', type=str, help='Experiment name(s) to execute.')


def main():
    parser = argparse.ArgumentParser(
        prog='experimenter',
        formatter_class=argparse.RawTextHelpFormatter,
        description='Start experiments'
    )
    retval = True
    add_args(parser)

    args = parser.parse_args()

    retval = experiment(args.experiments)

    if retval:
        prints('Experiment "{}" completed successfully.'.format(args.experiment))
    else:
        printe('Experiment "{}" experienced an error.'.format(args.experiment))


    if isinstance(retval, bool):
        exit(0 if retval else 1)
    elif isinstance(retval, int):
        exit(retval)
    else:
        exit(0 if retval else 1)


if __name__ == '__main__':
    main()