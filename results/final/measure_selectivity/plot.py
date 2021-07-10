import matplotlib.pyplot as plt
import matplotlib.dates as pltdates
import numpy as np
import pandas
import re

large = True
show = True
dest = 'tmp.pdf'


def col_to_name(name):
    match = re.search(r'instance="([0-9\.:]+)"', name)
    return match.group(1)

def _read(csv_path):
    return pandas.read_csv(csv_path, parse_dates=['Time'])
    

def diskio():
    df = _read('1/storage_diskio_selectivity1_512gb.csv')
    title = 'Disk I/O for 1% row selectivity'
    xlabel = 'Time (HH:MM)'
    ylabel = 'Disk usage (GiB/s)' 
    x = df['Time']
    ys = [df[colname]/1000/1000/1000 for colname in df.columns if colname != 'Time'] 
    labels = [col_to_name(colname) for colname in df.columns if colname != 'Time']
    return df, title, xlabel, ylabel, x, ys, labels


if large:
    fontsize = 28
    font = {
        'family' : 'DejaVu Sans',
        'size'   : fontsize
    }
    plt.rc('font', **font)

fig, ax = plt.subplots()


df, title, xlabel, ylabel, x, ys, labels = diskio()



# for colname in df.columns:
#     if colname != 'Time':
#         ax.plot(df[colname]/1000/1000, label=colname)

ax.stackplot(x, ys, labels=labels)

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

