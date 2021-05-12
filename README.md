# datacopy
python database data copier

data copy script, docker based, so you don't have to mess around with drivers :o)
multiprocess, so it can read fast and write faster!

reads the connections from one file (connections.csv), and queries from another.

the following env vars can be used to control it:
- CONNECTIONS_FILE: (default connections.csv)
- JOB_FILE: the csv file, default jobs.csv
- LOG_FILE: the output file, defaults to source.table.ok|ERROR.log
- TEST_QUERIES: set to 'yes' to only executes the select, and does not delete/write on destinations
- QUEUE_SIZE: (default 256) 
- REUSE_WRITERS: (default no)
- QUEUE_FB4NEWR: default 3, means that the buffer can be only 1/3 full before starting the next reader, if reusing writers.

See sample-* files to have an idea of usage.


### JOB_FILE COLUMNS

- source, dest: must match something on first column of connections.csv

- mode: 
    - caps mean close log file after this one
    - t means truncate, d delete before inserting
    - i means just insert.
    examples: 
        - T means truncate, insert data, and close log file. 
        - t means truncate, insert, leave log file open for next query
        - i just inserts, leave log file open for next query
        - I insert and close log file.

- query: can be a "select * from...", but if first char is @, means a file path to read a query from.
- table: destination table name.



### Optional Columns:

- fetch_size: how many rows per read, default 1000.

- parallel_writers: how many processes are launched to process the queue and to write to database. default 1.

- regexes: can be a placeholder/value, but if first char is @, reads placeolders [tab] values from a file

- insert_cols: can be a list of columns to build the insert statement, or:
    - @: build from source query column names (default option)
    - @l: build from source, lowering
    - @u: build from source, upping
    - anything else: comma delimited list of column names.


