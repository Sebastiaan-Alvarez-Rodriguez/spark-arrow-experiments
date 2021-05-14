import pandas
import pyarrow

import utils.fs as fs
from utils.printer import *
from data_generator.internal.data_format import DataFormat
from data_generator.internal.compression import Compression


def _gen_data(num_rows, num_columns, names=None):
     '''Generates data appearing as:

    val0, val1,   val2,   val3,   ..., valy
    x>>y, x>>y-1, x>>y-2, x>>y-3, ..., x

    Here, x is the row number, and y is the maximal column index.'''
    if names:
        if (not callable(names)) and len(names) != num_columns:
            raise ValueError('Provided names amount ({}) do not match required column name amount ({}): {}'.format(len(names), num_columns, names))
        if callable(names):
            names = [names(x) for x in range(num_columns)]
    else:
        names = ['val{}'.format(x) for x in num_columns]
    return {name: (x>>(num_columns-1-col_idx) for x in range(rows)) for col_idx, name in enumerate(names)}


def _pq(outputpath, stripe, num_columns, scheme_amount_bytes=int(1.5*1024*1024), compression=Compression.NONE, names=None):
    '''Generates parquet files.
    Args:
        outputpath (str): Path to create files in.
        stripe (int): Target filesize (in MB).
        num_columns (optional int): Number of columns to generate. Column names are generated as "col0, col1, col2...".
        scheme_amount_bytes (optional int): Expected parquet scheme size in MB.
                                            We keep this number of bytes available.
                                            If the scheme is larger, the data will not fit, and an error is printed.
        compression (optional Compression): Compression to use for data generation.

    Returns:
        `(True, num_generated_rows)` on success, `(False, None)` otherwise.'''
    import pyarrow.parquet as pq
    if compression == Compression.NONE:
        rows = (stripe*1024*1024-scheme_amount_bytes)//(8*num_columns)
    else:
        rows = (stripe*1024*1024-scheme_amount_bytes)//(4*num_columns)

    df = pandas.DataFrame(_gen_data(rows, num_columns, names=names))
    table = pyarrow.Table.from_pandas(df)

    # outputfile = fs.join(outputpath, 'num_{}MB_{}col.parquet'.format(stripe, num_columns))
    pq.write_table(table, outputpath, compression=compression.to_string())
    return True, rows


def _csv(outputpath, stripe, num_columns, names=None):
    '''Generates csv files.
    Args:
        outputpath (str): Path to create files in.
        stripe (int): Target filesize (in MB).
        num_columns (int): Number of columns to generate. Column names are generated as "col0, col1, col2...".
        compression (Compression): Compression to use for data generation.

    Returns:
        `(True, num_generated_rows)` on success, `(False, None)` otherwise.'''
    rows_initial = (stripe*1024*1024)//(num_columns) # rows if each entry was 1 char and 1 separator char.
    rows = rows_initial // len(str(rows//2)) # we divide this number by median entry len, to get a number of rows compensated for entry size. 
    df = pandas.DataFrame(_gen_data(rows, num_columns, names=names))
    df.to_csv(outputpath, sep=',', index=False, mode='w', line_terminator='\n', encoding='utf-8')
    return True, outputpath, rows


def register():
    return {
        DataFormat.PARQUET: _pq,
        DataFormat.CSV: _csv,
    }