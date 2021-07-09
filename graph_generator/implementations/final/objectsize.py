import re

import matplotlib.pyplot as plt
import numpy as np

from graph_generator.interface import GeneratorInterface
import graph_generator.internal.util.storer as storer
# from sklearn.metrics import r2_score

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
            ticks_arr.append(frame.identifiers['group'])
            local_i_arr = []
            local_c_arr = []
            local_label_arr = []
            local_errors_arr = []
            for frame in frames:
                local_i_arr.append(frame.i_avgtime)
                local_c_arr.append(frame.c_avgtime)
                local_label_arr.append(str(frame))
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
        width = 0.40


        bar_ind = np.array([])
        ticks_ind = []
        for idx, x in enumerate(plot_i_arr):
            tmp = np.arange(len(x)) if idx == 0 else np.arange(start=bar_ind[-1]+group_width, stop=bar_ind[-1]+group_width+len(x))
            bar_ind = np.concatenate((bar_ind, tmp), axis=None)
            ticks_ind.append(tmp[(len(tmp)-1)//2])
        bar_ind = bar_ind[:total_bars]
        # print(f'Bar indices: {bar_ind} (total:{len(bar_ind)}/{total_bars})')
        assert len(bar_ind) == total_bars
        # print(f'Ticks locations: {ticks_ind}')


        import itertools
        total_i_arr = list(itertools.chain(*plot_i_arr))
        ax.bar(bar_ind, total_i_arr, width, label='InitTime')
        ax.bar(bar_ind, list(itertools.chain(*plot_c_arr)), width, yerr=list(itertools.chain(*errors_arr)), bottom=total_i_arr, label='ComputeTime', align='center', alpha=0.5, ecolor='black', capsize=10*width)
        # Code below for normalization prediction.
        # z = np.polyfit(bar_ind, np.add(plot_i_arr,plot_c_arr), 1)
        # y_hat = np.poly1d(z)(bar_ind)
        # ax.plot(bar_ind, y_hat, '--', label='trend ($r^2$ = {:.3f})'.format(r2_score(np.add(plot_i_arr,plot_c_arr), y_hat)))

        ax.set(xlabel='Execution number', ylabel='Time (s)', title='Computation times')
        plt.xticks(ticks_ind, ticks_arr)
        ax.set_ylim(ymin=0)

        if large:
            ax.legend(loc='best', ncol=2, fontsize=18, frameon=False)
        else:
            ax.legend(loc='best', ncol=2, frameon=False)

        if large:
            fig.set_size_inches(16, 8)

        fig.tight_layout()

        if dest:
           storer.store_simple(dest, plt)

        if large:
            plt.rcdefaults()

        if show:
            plt.show()