# Data Generation
To quickly generate datasources, we use this data generator.
Execute it using:
```bash
python3 data_generator/entrypoint.py -h
```

## Adding a generator
To add a generator, write a Python script in [`/data/generator/implementations/`](/data/generator/implementations/).
The name of the file is the name for the generator.
Use the generator name in the CLI to load and execute it.

A generator script has a few requirements.
Basically, it should look like this:
```python
from data_generator.internal.data_format import DataFormat
from data_generator.internal.compression import Compression

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
    raise NotImplementedError

def _csv(outputpath, stripe, num_columns, names=None):
    '''Generates csv files.
    Args:
        outputpath (str): Path to create files in.
        stripe (int): Target filesize (in MB).
        num_columns (int): Number of columns to generate. Column names are generated as "col0, col1, col2...".
        compression (Compression): Compression to use for data generation.

    Returns:
        `(True, num_generated_rows)` on success, `(False, None)` otherwise.'''
    raise NotImplementedError


def register():
    return {
        DataFormat.PARQUET: _pq,
        DataFormat.CSV: _csv,
    }
```

The `register` function allows you to set functions for all supported data formats.
These functions will be called when the user specifies they want to generate given dataformat with this generator.
The parameters for the functions for all supported datatypes are shown and explained in the example.

By default, generated data is outputted to `/data_generator/generated/`.