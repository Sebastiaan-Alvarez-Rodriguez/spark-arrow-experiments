import os
import re

import matplotlib.pyplot as plt
import numpy as np

from graph_generator.interface import GeneratorInterface
import graph_generator.internal.util.storer as storer
from sklearn.metrics import r2_score
import scipy

# https://matplotlib.org/3.1.1/gallery/lines_bars_and_markers/bar_stacked.html


def get_generator(*args, **kwargs):
    return StackedBarPlot(*args, **kwargs)

def _get_numbers(string):
    m = re.match(r'cp([0-9]+)_ln([0-9]+)', string)
    return int(m.group(1)), int(m.group(2))

class StackedBarPlot(GeneratorInterface):
    '''Simple class to make lineplots.
    Expects that frames contain a `group` identifier. Each group category is plotted as a separate bar.'''
    def __init__(self, *args, **kwargs):
        pass


    def to_identifiers(self, path):
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

        cp, ln = _get_numbers(os.path.basename(os.path.dirname(path)))
        identifiers['size'] = cp * ln
        identifiers['group'] = str(identifiers['size'])
        return identifiers


    def sorting(self, frame):
        '''Sort result groups for grouped display.
        Args:
            frame (Frame): Frame to sort.

        Returns:
            `callable` sorting function to use when displaying results in a grouped manner.'''
        return frame.identifiers['size']

    def plot(self, frames, dest=None, show=True, large=False):
        plot_i_arr = []
        plot_c_arr = []
        label_arr = []
        ticks_arr = []
        errors_arr = []
        
        frames = list(frames)
        sort_func = frames[0].sort_func if any(frames) else self.sorting
        for frame in sorted(frames, key=sort_func):
            plot_i_arr.append(frame.i_avgtime)
            plot_c_arr.append(frame.c_avgtime)
            label_arr.append(str(frame))
            ticks_arr.append(frame.identifiers['size']*128//1024)
            # Code below for error whiskers (take note of percentile function to filter out outliers)
            normal_frame = (np.add(frame.c_arr,frame.i_arr))/1000000000
            percentile1 = np.percentile(normal_frame, 1)
            percentile99 = np.percentile(normal_frame, 99)
            errors_arr.append(np.std(normal_frame[np.where((percentile1 <= normal_frame) * (normal_frame <= percentile99))]))

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
        func = lambda x, a, b, c, d: a*x**3 + b*x**2 + c*x + d
        popt, pcov = scipy.optimize.curve_fit(func, ind, np.add(plot_i_arr,plot_c_arr))
        # z = np.polyfit(ind, np.add(plot_i_arr,plot_c_arr), 1)
        # y_hat = np.poly1d(z)(ind)
        # ax.plot(ind, y_hat, '--', label='trend ($r^2$ = {:.3f})'.format(r2_score(np.add(plot_i_arr,plot_c_arr), y_hat)))
        found_curve = func(ind, *popt)
        plt.plot(ind, found_curve, linestyle='--', marker='o', label='trend ($r^2$ = {:.3f})'.format(r2_score(np.add(plot_i_arr,plot_c_arr), found_curve)))

        ax.set(xlabel='Dataset size (GB)', ylabel='Time (s)', title='Data Scalability')
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