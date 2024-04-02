#pylint: disable=all
# pyright: reportUndefinedVariable=false
# pyright: reportMissingImports=false

'''just testings'''
from timeit import default_timer as timer

import pyarrow as pa

schema = pa.schema([("year", pa.int64()), ("col1", pa.int64()), ("col2", pa.float64())])


def open_parquet_2_write(p_schema:pa.table.schema):
    '''open file to write'''
    return pa.ParquetWriter('example2.parquet', p_schema)


def write_2_parquet(p_writer, p_table ):
    pass

def test1():
    '''test on array of functions speed'''
    pa.Table.from_batches()


   
    p_writer.write_table(table)


pf2 = pq.ParquetFile('example2.parquet')


def main():
    '''entry point'''
    start = timer()
    test1()
    print(f'test 1 took {timer()-start}')



if __name__ == '__main__':
    main()
