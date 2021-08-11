# datacopy

data copy script, docker based, so you don't have to mess around with drivers :o)

multiprocess, so it can read fast and write faster!

reads the connections from one file (connections.csv), and queries from another.

the following env vars can be used to control it:
- CONNECTIONS_FILE: (default connections.csv)
- JOB_FILE: the csv file (default jobs.csv)
- LOG_FILE: the output file (defaults to dest.table.ok|ERROR.log)
- TEST_QUERIES: set to 'yes' to only execute the select, and does not delete/write on destinations
- QUEUE_SIZE: (default 256) 
- REUSE_WRITERS: (default no)
- QUEUE_FB4NEWR: default 3, means that the buffer can be only 1/3 full before starting the next reader, if reusing writers.
- STOP_JOBS_ON_ERROR (default yes)

See sample-* files to have an idea of usage.


### JOB_FILE COLUMNS (tab delimited)

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

- query: can be a "select * from...", but if first char is @, means a file path to read a query from. or it can be just a table name, and it will build automatically a SELECT "[col1]","[col2]",(..)"[colN]" from [table].
- table: destination table name.


### Optional Columns:

- fetch_size: how many rows per read, default 1000.

- parallel_writers: how many processes are launched to process the queue and to write to database. default 1.

- regexes: can be a placeholder/value, but if first char is @, reads placeolders values from a file, tab delimited

- insert_cols: can be a list of columns to build the insert statement (comma delimited), or:
    - @: build from source query column names (default option)
    - @l: build from source, lowering case
    - @u: build from source, upping case
    - anything else: comma delimited list of column names.

- ignore_cols: a list of column names, separated by comma. only used it the query field does not have a select statement, just a table name.

## stats
if you grep -E '^stats:' \*.log, you can get some info about lines read and wrote.

example (csv with 8 jobs, with REUSE_WRITERS=yes):
```
stats:read:1:2606409:29.33:1:20210806071738.140707
stats:read:2:11221622:138.46:1:20210806072136.663124
stats:read:3:2310055:26.87:1:20210806072224.235138
stats:read:4:309374:3.86:1:20210806072231.196543
stats:read:5:24836387:288.38:1:20210806073109.770731
stats:read:6:7174513:91.45:1:20210806073405.059415
stats:read:7:9938499:126.50:1:20210806073757.953427
stats:read:8:14102901:163.66:1:20210806074255.200570
stats:write:8:72499760:10782.49:3:20210806074256.784539
```

(colon is used as separator, because if you grep a bunch of files, you will get the file name as first column)
fields:
- jobID (line number on jobs.csv file)
- lines read or written
- seconds: total seconds of read or write operations. on multi thread, it means you need to split per thread count.
- thread count
- timestamp
