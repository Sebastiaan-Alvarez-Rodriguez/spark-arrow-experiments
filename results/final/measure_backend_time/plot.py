import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
from matplotlib.colors import ListedColormap


def nvme_stats():
    base_spark = 5.01 # 04-08
    base_arrowspark = 14.8 # 14.8-23
    base_arrowspark_offloading = 14.9#15-23

    rel_spark = base_spark
    rel_arrowspark = base_arrowspark-base_spark
    rel_arrowspark_offloading = base_arrowspark_offloading-base_arrowspark
    labels = ['Spark', 'Overhead Arrow-Spark', 'Overhead Offloading']
    sizes = [rel_spark, rel_arrowspark, rel_arrowspark_offloading]
    # colors = ['gold', 'yellowgreen', 'lightcoral'] #, 'lightskyblue']
    return labels, sizes, 'Execution Time Composition on CephFS.'


def cephfs_stats():
    base_spark = 8.4
    base_arrowspark = 35.4
    base_arrowspark_offloading = 37.0

    rel_spark = base_spark
    rel_arrowspark = base_arrowspark-base_spark
    rel_arrowspark_offloading = base_arrowspark_offloading-base_arrowspark
    labels = ['Spark', 'Overhead Arrow-Spark', 'Overhead Offloading']
    sizes = [rel_spark, rel_arrowspark, rel_arrowspark_offloading]
    # colors = ['gold', 'yellowgreen', 'lightcoral'] #, 'lightskyblue']
    return labels, sizes, 'Execution Time Composition on CephFS.'


def uncompressed_IPC_backend_stats():
    stat_fragment = 1.033176
    serialize_scanrequest = 0.162723
    deserialize_scanrequest = 3.283626
    disk_io = 106.314810
    scan_pq_data = 970.315510
    serialize_resulttable = 1322.364127
    transfer = 782.443831
    deserialize_resulttable = 0.272527
    sizes = [stat_fragment, serialize_scanrequest, deserialize_scanrequest, disk_io, scan_pq_data, serialize_resulttable, transfer, deserialize_resulttable]
    labels = ['Stat Fragment', 'Serialize Scan Request', 'Deserialize Scan Request', 'Disk I/O', 'Scan Parquet Data', 'Serialize Result Table', 'Result Transfer', 'Deserialize Result Table']
    labels = ['[{}] {} ({:.02f}%)'.format(idx, label, size/sum(sizes)*100) for idx, (label, size) in enumerate(zip(labels, sizes))]
    return labels, sizes, 'Execution Time Composition for Data Requests.'


def compressed_IPC_backend_stats():
    stat_fragment = 0.595635
    serialize_scanrequest = 0.151158
    deserialize_scanrequest = 1.005940
    disk_io = 106.292543
    scan_pq_data = 946.938071
    serialize_resulttable = 577.691875
    transfer = 229.385381
    deserialize_resulttable = 105.408662
    sizes = [stat_fragment, serialize_scanrequest, deserialize_scanrequest, disk_io, scan_pq_data, serialize_resulttable, transfer, deserialize_resulttable]
    labels = ['Stat Fragment', 'Serialize Scan Request', 'Deserialize Scan Request', 'Disk I/O', 'Scan Parquet Data', 'Serialize Result Table', 'Result Transfer', 'Deserialize Result Table']
    labels = ['[{}] {} ({:.02f}%)'.format(idx, label, size/sum(sizes)*100) for idx, (label, size) in enumerate(zip(labels, sizes))]
    return labels, sizes, 'Execution Time Composition for Data Requests.'


def serialization_first_stats():
    __memmove_sse2_unaligned_erms = 48.98
    clear_page_erms = 9.14
    native_irq_return_iret = 5.60
    __memset_avx2_erms = 3.91
    sync_regs = 3.90
    rmqueue = 1.98
    __pagevec_lru_add_fn = 1.80
    handle_mm_fault = 1.41
    __handle_mm_fault = 1.34
    get_mem_cgroup_from_mm = 1.32
    try_charge = 1.26
    do_anonymous_page = 0.99
    sizes = [__memmove_sse2_unaligned_erms, clear_page_erms, native_irq_return_iret, __memset_avx2_erms, sync_regs, rmqueue, __pagevec_lru_add_fn, handle_mm_fault, __handle_mm_fault, get_mem_cgroup_from_mm, try_charge, do_anonymous_page]
    others = 100-sum(sizes)
    sizes.append(others)
    labels = [' __memmove_sse2_unaligned_erms', 'clear_page_erms', 'native_irq_return_iret', ' __memset_avx2_erms', 'sync_regs', 'rmqueue', ' __pagevec_lru_add_fn', 'handle_mm_fault', ' __handle_mm_fault', 'get_mem_cgroup_from_mm', 'try_charge', 'do_anonymous_page', 'others']
    labels = ['{} ({:.02f}%)'.format(label, size) for (label, size) in zip(labels, sizes)]
    return *sorter(labels, sizes), 'Execution Time Composition for Data Serialization.'
# https://lists.apache.org/thread.html/r33da4ac6261269e1c1da7feaaf39393d898b9aab3827d0c9fcd78141%40%3Cdev.arrow.apache.org%3E

def serialization_cached_stats():
    __memmove_sse2_unaligned_erms = 98.40
    __mod_zone_page_state = 0.44
    zap_pte_range_isra_0 = 0.44
    __memset_avx2_erms = 0.40
    arrow_StringArray_StringArray = 0.17
    cpuacct_account_field = 0.16
    sizes = [__memmove_sse2_unaligned_erms, __mod_zone_page_state, zap_pte_range_isra_0, __memset_avx2_erms, arrow_StringArray_StringArray, cpuacct_account_field]
    others = 100-sum(sizes)
    sizes.append(others)
    labels = [' __memmove_sse2_unaligned_erms', ' __mod_zone_page_state', 'zap_pte_range.isra.0', ' __memset_avx2_erms', 'arrow::StringArray::~StringArray', 'cpuacct_account_field']
    labels = ['{} ({:.02f}%)'.format(label, size) for (label, size) in zip(labels, sizes)]
    return *sorter(labels, sizes), 'Execution Time Composition for Data Serialization, cached.'


def sorter(labels, sizes):
    '''Takes a label list and size list. Returns them after sorting everything from highest to lowest.'''
    zipped_lists = zip(sizes, labels)
    zipped_lists = sorted(zipped_lists, reverse=True)
    return [x for (_,x) in zipped_lists], [x for (x,_) in zipped_lists]


labels, sizes, title = serialization_cached_stats()
# colorscheme = plt.get_cmap('twilight_shifted')#cm.Set1(range(len(sizes)))
# colorscheme = plt.get_cmap('tab20c')
# colorscheme = plt.get_cmap('ocean')
# colorscheme = plt.get_cmap('gist_earth')
# colorscheme = plt.get_cmap('rainbow')
colorscheme = plt.get_cmap('viridis')
# colorscheme = plt.get_cmap('Greens') 
# my_cs = colorscheme(np.arange(colorscheme.N))
# my_cs[:, -3] = np.linspace(0, 1, colorscheme.N)
# my_cs = ListedColormap(my_cs)

fig1, ax1 = plt.subplots()
ax1.set_prop_cycle('color', [colorscheme(1. * i / len(sizes)) for i in range(len(sizes))])

pie = ax1.pie(sizes, shadow=False, startangle=140, wedgeprops = {'edgecolor' : 'white'})
plt.axis('equal')
plt.legend(pie[0], labels, bbox_to_anchor=(1,0.5), loc="upper right", bbox_transform=plt.gcf().transFigure)
plt.subplots_adjust(left=0.0, bottom=0.1, right=0.45)
plt.title(title)
plt.savefig('serialization_composition_cached.pdf', bbox_inches='tight')
plt.show()

# TODO: later try: https://www.tutorialspoint.com/plotly/plotly_quick_guide.htm (find the plotly piechart in there)