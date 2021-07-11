from matplotlib import cm
import matplotlib.pyplot as plt
import matplotlib.dates as pltdates
import numpy as np
import pandas
import re

large = True
show = True
dest = None #'net_selectivity_1_512gb_no_offload.pdf'


def col_to_name(name):
    match = re.search(r'instance="([0-9\.:]+)"', name)
    return match.group(1)

def _read(csv_path):
    return pandas.read_csv(csv_path, parse_dates=['Time']).fillna(0)

def time_seconds(df):
    return (df['Time'].iloc[-1] - df['Time'].iloc[0]).total_seconds()

def area_under_curve(df):
    from scipy.integrate import simps
    # df.sum(axis=1, numeric_only=True)
    # df['sumcolumn'] = sum(df[name] for name in df.columns if name != 'Time').fillna(0)
    # print(df['sumcolumn'])
    area = simps(df.sum(axis=1, numeric_only=True)/1024/1024, dx=15)
    print(f'Area={area}')
    print(f'Seconds={time_seconds(df)}')



def diskio():
    df = _read('1/storage_diskio_selectivity1_512gb.csv')
    title = 'Storage Disk I/O for 1% row selectivity'
    xlabel = 'Time (HH:MM)'
    ylabel = 'Disk usage (MiB/s)' 
    x = df['Time']
    ys = [df[colname]/1000/1000 for colname in df.columns if colname != 'Time'] 
    labels = [col_to_name(colname) for colname in df.columns if colname != 'Time']
    colormap = cm.get_cmap('viridis')
    colors = [colormap(i) for i in np.linspace(0.0, 0.75, num=len(df.columns)-1)]
    return df, title, xlabel, ylabel, x, ys, labels, colors


def netio():
    df = _read('1/client_network_selectivity1_512gb.csv')
    title = 'Client Network I/O for 1% row selectivity'
    xlabel = 'Time (HH:MM)'
    ylabel = 'Network usage (MiB/s)' 
    x = df['Time']
    ys = [df[colname]/1000/1000 for colname in df.columns if colname != 'Time'] 
    labels = [col_to_name(colname) for colname in df.columns if colname != 'Time']
    colormap = cm.get_cmap('viridis')
    colors = [colormap(i) for i in np.linspace(0.0, 0.75, num=len(df.columns)-1)]
    return df, title, xlabel, ylabel, x, ys, labels, colors


def netio100():
    df = _read('100/client_network_selectivity100_512gb.csv')
    title = 'Client Network I/O for 1% row selectivity, no offloading'
    xlabel = 'Time (HH:MM)'
    ylabel = 'Network usage (MiB/s)' 
    x = df['Time']
    ys = [df[colname]/1000/1000 for colname in df.columns if colname != 'Time'] 
    labels = [col_to_name(colname) for colname in df.columns if colname != 'Time']
    colormap = cm.get_cmap('viridis')
    colors = [colormap(i) for i in np.linspace(0.0, 0.75, num=len(df.columns)-1)]
    return df, title, xlabel, ylabel, x, ys, labels, colors


def cpu_client():
    df = _read('1/client_cpu_selectivity1_512gb.csv')
    title = 'Compute cluster CPU utilization for 1% row selectivity'
    xlabel = 'Time (HH:MM)'
    ylabel = 'CPU utilization (% of total)' 
    x = df['Time']
    ys = [df[colname] for colname in df.columns if colname != 'Time'] 
    labels = [col_to_name(colname) for colname in df.columns if colname != 'Time']
    colormap = cm.get_cmap('viridis')
    colors = [colormap(i) for i in np.linspace(0.0, 0.75, num=len(df.columns)-1)]
    return df, title, xlabel, ylabel, x, ys, labels, colors


def cpu_storage():
    df = _read('1/storage_cpu_selectivity1_512gb.csv')
    title = 'Compute cluster CPU utilization for 1% row selectivity'
    xlabel = 'Time (HH:MM)'
    ylabel = 'CPU utilization (% of total)' 
    x = df['Time']
    ys = [df[colname] for colname in df.columns if colname != 'Time'] 
    labels = [col_to_name(colname) for colname in df.columns if colname != 'Time']
    colormap = cm.get_cmap('viridis')
    colors = [colormap(i) for i in np.linspace(0.0, 0.75, num=len(df.columns)-1)]
    return df, title, xlabel, ylabel, x, ys, labels, colors


if large:
    fontsize = 28
    font = {
        'family' : 'DejaVu Sans',
        'size'   : fontsize
    }
    plt.rc('font', **font)

fig, ax = plt.subplots()


df, title, xlabel, ylabel, x, ys, labels, colors = diskio()
area_under_curve(df)


# for colname in df.columns:
#     if colname != 'Time':
#         ax.plot(df[colname]/1000/1000, label=colname)

ax.stackplot(x, ys, labels=labels, colors=colors)

fmt = pltdates.DateFormatter('%H:%M')
ax.xaxis.set_major_formatter(fmt)
plt.gcf().autofmt_xdate()

ax.set(xlabel=xlabel, ylabel=ylabel, title=title)
ax.set_ylim(ymin=0)


if large:
    ax.legend(loc='best', fontsize=18, frameon=False, bbox_to_anchor=(1.07, 0.75), bbox_transform=plt.gcf().transFigure)
else:
    ax.legend(loc='best', frameon=False, bbox_to_anchor=(1, 1))

if large:
    fig.set_size_inches(16, 8)

fig.tight_layout()

if dest:
   plt.savefig(dest, bbox_inches='tight')

if large:
    plt.rcdefaults()

if show:
    plt.show()

