import re

import matplotlib.pyplot as plt
import numpy as np

from graph_generator.interface import GeneratorInterface
import graph_generator.internal.util.storer as storer


def get_generator(*args, **kwargs):
    return LinePlot(*args, **kwargs)


class LinePlot(GeneratorInterface):
    '''Simple class to make lineplots.'''
    def __init__(self, *args, **kwargs):
        pass

    def to_identifiers(self, path):
        identifiers = dict()
        if path.endswith('.res_a'):
            identifiers['producer'] = 'arrow'
        elif path.endswith('.res_s'):
            identifiers['producer'] = 'spark'

        exp_found = re.search(r'exp[0-9]+', path)
        if exp_found:
            identifiers['exp'] = path[exp_found.start():exp_found.end()]
        rs_found = re.search(r'_rs([0-9]+)', path)
        if rs_found:
            identifiers['selectivity'] = path[rs_found.start()+3:rs_found.end()]+'%'
        return identifiers


    def plot(self, frames, dest=None, show=True, large=False):
        plot_arr = []
        label_arr=[]
        for frame in frames:
            plot_arr.append(frame.c_arr / 1000000000)
            label_arr.append(str(frame))
            # ovars = Dimension.open_vars(num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)[0]
            # label_arr.append((
            #     'Arrow-Spark: {}'.format(Dimension.make_id_string(frame_arrow, num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)),
            #     'Spark: {}'.format(Dimension.make_id_string(frame_spark, num_cols, compute_cols, node, partitions_per_node, extension, compression, amount, kind, rb)),))
        if large:
            fontsize = 28
            font = {
                'family' : 'DejaVu Sans',
                'size'   : fontsize
            }
            plt.rc('font', **font)

        fig, ax = plt.subplots()
        for data, label in zip(plot_arr, label_arr):
            ax.plot(data, label=label)

        ax.set(xlabel='Execution number', ylabel='Time (s)', title='Computation times')
        ax.set_ylim(ymin=0)
        if large:
            ax.legend(loc='best', fontsize=18, frameon=False)
        else:
            ax.legend(loc='best', frameon=False)

        if large:
            fig.set_size_inches(16, 8)

        fig.tight_layout()

        if dest:
           storer.store_simple(dest, plt)

        if large:
            plt.rcdefaults()

        if show:
            plt.show()