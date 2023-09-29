# parquet-maria

## Current - parsing an example parquet file generated from python script below
sauce: https://arrow.apache.org/docs/python/parquet.html#reading-and-writing-single-files
```python
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                   'two': ['foo', 'bar', 'baz'],
                   'three': [True, False, True]},
                   index=list('abc'))
table = pa.Table.from_pandas(df)
pq.write_table(table, 'example.parquet')
```
