import itertools
import os
import re

import matplotlib.pyplot as plt
from matplotlib import cm
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
    '''Simple class to make barplots.
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
        identifiers['group1'] = str(identifiers['producer'])
        return identifiers


    def sorting(self, frame):
        '''Sort result groups for grouped display.
        Args:
            frame (Frame): Frame to sort.

        Returns:
            `callable` sorting function to use when displaying results in a grouped manner.'''
        return (frame.identifiers['size'], frame.identifiers['producer'])


    def plot(self, frames, dest=None, show=True, large=False):
        plot_i_arr = []
        plot_c_arr = []
        label_arr = []
        ticks_arr = []
        errors_arr = []

        if large:
            fontsize = 28
            font = {
                'family' : 'DejaVu Sans',
                'size'   : fontsize
            }
            plt.rc('font', **font)

        fig, ax = plt.subplots()


        frames = list(frames)
        sort_func = frames[0].sort_func if any(frames) else self.sorting
        frames.sort(key=sort_func)

        grouped_frames = dict()
        for frame in frames:
            if frame.identifiers['group'] in grouped_frames:
                grouped_frames[frame.identifiers['group']].append(frame)
            else:
                grouped_frames[frame.identifiers['group']] = [frame]


        for group in grouped_frames.keys():
            frames_found = grouped_frames[group]
            ticks_arr.append(f'{int(group)*128//1024}')
            local_i_arr = []
            local_c_arr = []
            local_label_arr = []
            local_errors_arr = []
            for frame in frames_found:
                local_i_arr.append(frame.i_avgtime)
                local_c_arr.append(frame.c_avgtime)
                local_label_arr.append(frame.identifiers['producer']) # frame.identifiers['size']*128//1024
                # Code below for error whiskers (take note of percentile function to filter out outliers)
                normal_frame = (np.add(frame.c_arr,frame.i_arr))/1000000000
                percentile1 = np.percentile(normal_frame, 1)
                percentile99 = np.percentile(normal_frame, 99)
                local_errors_arr.append(np.std(normal_frame[np.where((percentile1 <= normal_frame) * (normal_frame <= percentile99))]))
            plot_i_arr.append(local_i_arr)
            plot_c_arr.append(local_c_arr)
            label_arr.append(local_label_arr)
            errors_arr.append(local_errors_arr)

        total_bars = sum(len(x) for x in plot_i_arr)
        print(f'Have {total_bars} bars, {len(grouped_frames)} groups.')
        group_width = 3.20
        # width = 0.40
        width = 0.8 / len(plot_i_arr)

        bar_ind = np.array(range(len(plot_i_arr)))
        ticks_ind = [x + (width * (len(bar_ind)-1) * 0.5) for x in bar_ind]


        colormap = cm.get_cmap('winter')
        colors = [colormap(i) for i in np.linspace(0.0, 0.75, num=len(plot_i_arr[0]))]


        print(f'plot_i_arr: {plot_i_arr}\nplot_c_arr: {plot_c_arr}\n')
        print(f'colors: {colors}')

        legend = []
        for i, c in enumerate(colors):
            total_i_arr = []
            total_c_arr = []
            total_err = []
            total_label = []
            ind_arr = []
            for j in range(len(bar_ind)):
                total_i_arr.append(plot_i_arr[j][i])
                total_c_arr.append(plot_c_arr[j][i])
                total_err.append(errors_arr[j][i])
                ind_arr.append(bar_ind[j] + i * width) 
                # ax.bar(bar_ind[j] + i * width, plot_i_arr[j][i], width, color=c, label=label_arr[0][i])
                # ax.bar(bar_ind[j] + i * width, plot_c_arr[j][i], width, yerr=errors_arr[j][i], bottom=plot_i_arr[j][i], label=label_arr[0][i],align='center', alpha=0.6, ecolor='black', capsize=width*50, color=c)
            legend.append((ax.bar(ind_arr, total_i_arr, width, color=c), f'InitTime ({label_arr[0][i]})'))
            legend.append((ax.bar(ind_arr, total_c_arr, width, yerr=total_err, bottom=total_i_arr, color=c, align='center', alpha=0.6, ecolor='black', capsize=width*50), f'CompTime ({label_arr[0][i]})'))


        arrow_points = [np.median(np.add(frame.i_arr, frame.c_arr)) for frame in frames if frame.identifiers['producer'] == 'arrow']
        spark_points = [np.median(np.add(frame.i_arr, frame.c_arr)) for frame in frames if frame.identifiers['producer'] == 'spark']
        # spark_points = [frame.total_avgtime for frame in grouped_frames['spark']]
        print(f'Num points: {len(arrow_points)}')
        print(f'Num arrow frames: {len([1 for x in frames if x.identifiers["producer"] == "arrow"])}')
        print(f'Num spark frames: {len([1 for x in frames if x.identifiers["producer"] == "spark"])}')
        ax2 = ax.twinx()
        y_label = 'Relative speedup\nof Spark' if large else 'Relative speedup of Spark'
        ax2.set_ylabel(y_label)
        ax2.tick_params(axis='y', colors='red')
        ax2.plot(np.arange(len(arrow_points)), np.divide(arrow_points, spark_points), label=y_label, marker='D', markersize=10, markeredgecolor='black', markeredgewidth='1.5', color='red')
        ax2.grid()

        ax.set(xlabel='Dataset size (GiB)', ylabel='Time (s)', title='Data Scalability')
        plt.xticks(bar_ind, ticks_arr)
        ax.set_ylim(ymin=0)
        ax2.set_ylim(ymin=0, top=5.3)

        if large:
            ax2.legend([x for x,_ in legend], [x for _,x in legend], loc='upper left', facecolor='white', framealpha=1, fontsize=18, frameon=False, ncol=2)
        else:
            ax2.legend([x for x,_ in legend], [x for _,x in legend], loc='upper left', facecolor='white', framealpha=1, frameon=False, ncol=2)

        if large:
            fig.set_size_inches(16, 8)

        fig.tight_layout()

        if dest:
           storer.store_simple(dest, plt)

        if large:
            plt.rcdefaults()

        if show:
            plt.show()