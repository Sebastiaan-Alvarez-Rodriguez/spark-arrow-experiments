import re

import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np

from graph_generator.interface import GeneratorInterface
import graph_generator.internal.util.storer as storer
from matplotlib.patches import Rectangle
# from sklearn.metrics import r2_score

# https://matplotlib.org/3.1.1/gallery/lines_bars_and_markers/bar_stacked.html


def get_generator(*args, **kwargs):
    return StackedBarPlot(*args, **kwargs)


class StackedBarPlot(GeneratorInterface):
    '''Simple class to make lineplots.
    Expects that frames contain a `group` identifier. Each group category is plotted as a separate bar.'''
    def __init__(self, *args, **kwargs):
        pass


    def plot(self, frames, dest=None, show=True, large=False):
        plot_i_arr = []
        plot_c_arr = []
        label_arr = []
        ticks_arr = []
        errors_arr = []

        frames = list(frames)
        sort_func = frames[0].sort_func if any(frames) else self.sorting
        frames.sort(key=sort_func)


        grouped_frames = dict()
        for frame in frames:
            if frame.identifiers['group'] in grouped_frames:
                grouped_frames[frame.identifiers['group']].append(frame)
            else:
                grouped_frames[frame.identifiers['group']] = [frame]

        if large:
            fontsize = 28
            font = {
                'family' : 'DejaVu Sans',
                'size'   : fontsize
            }
            plt.rc('font', **font)

        fig, ax = plt.subplots()

        for group in grouped_frames.keys():
            frames = grouped_frames[group]
            ticks_arr.append(f'{group}%')
            local_i_arr = []
            local_c_arr = []
            local_label_arr = []
            local_errors_arr = []
            for frame in frames:
                local_i_arr.append(frame.i_avgtime)
                local_c_arr.append(frame.c_avgtime)
                local_label_arr.append(frame.identifiers['objectsize'])
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


        # bar_ind = np.array([])
        # ticks_ind = []
        # for idx, x in enumerate(plot_i_arr):
        #     tmp = np.arange(len(x)) if idx == 0 else np.arange(start=bar_ind[-1]+group_width, stop=bar_ind[-1]+group_width+len(x))
        #     bar_ind = np.concatenate((bar_ind, tmp), axis=None)
        #     ticks_ind.append(tmp[(len(tmp)-1)//2])
        # bar_ind = bar_ind[:total_bars]
        # # print(f'Bar indices: {bar_ind} (total:{len(bar_ind)}/{total_bars})')
        # assert len(bar_ind) == total_bars
        # print(f'Ticks locations: {ticks_ind}')

        bar_ind = np.array(range(len(plot_i_arr)))
        ticks_ind = [x + (width * (len(bar_ind)-1) * 0.5) for x in bar_ind]


        import itertools

        colormap = cm.get_cmap('winter')
        # colormap = cm.get_cmap('twilight_shifted')
        # colormap = cm.get_cmap('Dark2')
        # colormap = cm.get_cmap('Set1')
        # colormap = cm.get_cmap('tab10')
        # colormap = cm.get_cmap('brg') # prob want to do linspace(0.0, 0.75) or s.thing
        # colormap = cm.get_cmap('jet')
        # colormap = cm.get_cmap('turbo')
        ## colormap = cm.get_cmap('viridis')
        # colormap = cm.get_cmap('summer') #linspace(0.0, 0.8)
        # colormap = cm.get_cmap('ocean') # linspace(0.0, 0.8)
        # TODO: pick any range [0.0, 1.0] to get a portion of the colormap
        colors = [colormap(i) for i in np.linspace(0.0, 0.75, num=len(plot_i_arr[0]))]
        print(colors)
        # colors = ['red', 'green', 'blue', 'purple', 'orange', 'violet']
        enumerated = [i for i in range(len(plot_i_arr))]


        total_i_arr = list(itertools.chain(*plot_i_arr))
        print(plot_i_arr)
        legend_handle1 = []
        legend_handle2 = []
        legend_row = ['']
        legend_row1 = [['Init.'], ['Comp.']]
        label_empty = ['']
        extra = Rectangle((0, 0), 1, 1, fc="w", fill=False, edgecolor='none', linewidth=0)
        legend_handle = [extra for x in enumerated]
        for i, c in zip(enumerated, colors):
            for j in range(len(bar_ind)):
                bar1 = ax.bar(bar_ind[j] + i * width, plot_i_arr[j][i], width, color=c)
                bar2 = ax.bar(bar_ind[j] + i * width, plot_c_arr[j][i], width, yerr=errors_arr[j][i], bottom=plot_i_arr[j][i], align='center', alpha=0.6, ecolor='black', capsize=width*50, color=c)
            # legend_handle1.append(ax.bar(bar_ind + i * width, total_i_arr[i], width, color=c))
            # legend_handle2.append(ax.bar(bar_ind + i * width, list(itertools.chain(*plot_c_arr))[i], width, yerr=list(itertools.chain(*errors_arr))[i], bottom=total_i_arr[i], align='center', alpha=0.5, ecolor='black', capsize=width*50, color=c))
            legend_row.append(f'{label_arr[0][i]} MB')
            legend_handle1.append(bar1)
            legend_handle2.append(bar2)


        legend_handle.append(extra)
        legend_handle.extend(legend_handle1)
        legend_handle.append(extra)
        legend_handle.extend(legend_handle2)
        # Code below for normalization prediction.
        # z = np.polyfit(bar_ind, np.add(plot_i_arr,plot_c_arr), 1)
        # y_hat = np.poly1d(z)(bar_ind)
        # ax.plot(bar_ind, y_hat, '--', label='trend ($r^2$ = {:.3f})'.format(r2_score(np.add(plot_i_arr,plot_c_arr), y_hat)))

        empty_len = len(enumerated)-1

        # inspiration: https://stackoverflow.com/questions/25830780/tabular-legend-layout-for-matplotlib
        legend_labels = np.concatenate([legend_row, legend_row1[0], label_empty*empty_len, legend_row1[1], label_empty*empty_len])

        ax.set(xlabel='Row selectivity', ylabel='Time (s)', title='Computation times')
        plt.xticks(ticks_ind, ticks_arr)
        ax.set_ylim(ymin=0)

        if large: 
            ax.legend(legend_handle, legend_labels, loc='best', ncol=3, shadow=True, handletextpad=-2, bbox_to_anchor=(1, 1), fontsize=18, title='Object sizes')
        else:
            ax.legend(legend_handle, legend_labels, loc='best', ncol=3, shadow=True, handletextpad=-2, bbox_to_anchor=(1, 1), title='Object sizes')

        if large:
            fig.set_size_inches(16, 8)

        fig.tight_layout()

        if dest:
           storer.store_simple(dest, plt, bbox_inches='tight')

        if large:
            plt.rcdefaults()

        if show:
            plt.show()