import pandas
import pyarrow
import pyarrow.parquet as pq

import internal.util.fs as fs
from internal.util.printer import *

def generate(outputpath, stripe, scheme_amount_bytes=int(1.5*1024*1024), columns=4):
    '''Generates data appearing as:

    val0, val1,   val2,   val3,   ..., valy
    x>>y, x>>y-1, x>>y-2, x>>y-3, ..., x

    Here, x is the row number (8 bytes int), and y is the maximal column index.
    Args:
        outputpath (str): Path to create files in.
        stripe (int): Target filesize (in MB)

    Returns:
        `(True, generated_filepath)` on success, `(False, None)` otherwise.'''
    columns = 4

    # E.g: ("stripe" MB-sizeof(scheme))/sizeof(1 row filled with ints) to get a datasize of "stripe" MB.
    # Then, parquet adds a bit of space for the scheme.
    # We end up with a parquet file of just a bit less than 64 MB, as long as we carefully pick our scheme-bytes reservation.
    rows = (stripe*1024*1024-scheme_amount_bytes)//(8*columns)

    data = {'val{}'.format(c): (x>>(columns-1-c) for x in range(rows)) for c in range(columns)}

    df = pandas.DataFrame(data)

    table = pyarrow.Table.from_pandas(df)

    outputfile = fs.join(outputpath, 'num_{}MB_{}col.parquet'.format(stripe, columns))
    pq.write_table(table, outputfile, compression=None)
    print('Wrote {} rows'.format(rows))
    return True, outputfile