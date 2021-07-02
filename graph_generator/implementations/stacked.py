import re

import matplotlib.pyplot as plt
import numpy as np

from graph_generator.interface import GeneratorInterface
import graph_generator.internal.util.storer as storer
from sklearn.metrics import r2_score

# https://matplotlib.org/3.1.1/gallery/lines_bars_and_markers/bar_stacked.html


def get_generator(*args, **kwargs):
    return StackedBarPlot(*args, **kwargs)


class StackedBarPlot(GeneratorInterface):
    '''Simple class to make lineplots.
    Expects that frames contain a `group` identifier. Each group category is plotted as a separate bar.'''
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
            identifiers['group'] = identifiers['selectivity']
        return identifiers


    def sorting(self, frame):
        return (int(e.identifiers['selectivity'][:-1]), e.identifiers['exp'], e.identifiers['producer'])


    def plot(self, frames, dest=None, show=True, large=False):
        plot_i_arr = []
        plot_c_arr = []
        label_arr = []
        ticks_arr = []
        errors_arr = []
        
        frames = list(frames)
        sort_func = frames[0].sort_func if any(frames) else lambda e: 0
        print(f'Frame: {frames[0]}. Sorter: {sort_func(frames[0])}, func {sort_func}')
        for frame in sorted(frames, key=sort_func):
            plot_i_arr.append(frame.i_avgtime)
            plot_c_arr.append(frame.c_avgtime)
            label_arr.append(str(frame))
            ticks_arr.append(frame.identifiers['group'])
            # Code below for error whiskers (take note of percentile function to filter out outliers)
            normal_frame = (np.add(frame.c_arr,frame.i_arr))/1000000000
            percentile = np.percentile(normal_frame, 99)
            errors_arr.append(np.std(normal_frame[normal_frame <= percentile]))

        if large:
            fontsize = 28
            font = {
                'family' : 'DejaVu Sans',
                'size'   : fontsize
            }
            plt.rc('font', **font)

        fig, ax = plt.subplots()

        ind = np.arange(len(label_arr))
        width = 0.20
        ax.bar(ind, plot_i_arr, width, label='InitTime')
        ax.bar(ind, plot_c_arr, width, yerr=errors_arr, bottom=plot_i_arr, label='ComputeTime', align='center', alpha=0.5, ecolor='black', capsize=10)
        # Code below for normalization prediction.
        z = np.polyfit(ind, np.add(plot_i_arr,plot_c_arr), 1)
        y_hat = np.poly1d(z)(ind)
        ax.plot(ind, y_hat, '--', label='trend')
        # Code below for r-squared analytical deviation from normalization prediction.
        plt.annotate("r-squared = {:.3f}".format(r2_score(np.add(plot_i_arr,plot_c_arr), y_hat)), (0, 1))

        ax.set(xlabel='Execution number', ylabel='Time (s)', title='Computation times')
        plt.xticks(ind, ticks_arr)
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