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
- DUMPFILE_SEP (default | )
- STATS_IN_JSON (default no)
- DEBUG (default no, yes to get a lot of detaul on stderr, and a DUMP of the block that fails on writes)

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

- pre_query_src, pre_query_dst: like query, but will run on source or destinations connections before other statements. for things like "set dateformat ymd", for instance...

-- csv_encode_special: when we have \n or \t or other control characters on data, it may mess up the csv file. 
    this option will translate special chars (ascill < 32 to \0x string representation)
    this translation is always applied to DUMP files. 

-- override_insert_placeholder: to override the default %s insert placeholder. Found that some azure stuff need a ? instead...

## stats
a .stats file is created on the same folder as the log file. can be a csv (tab delimited) or in json format.

example (csv with 8 jobs, with REUSE_WRITERS=yes):
```
20210812195246.279088	execQuery	1	0	5.01	0
20210812195247.533628	read	1	76471	0.89	1
20210812195338.072325	execQuery	2	0	50.50	0
20210812195433.103919	read	2	506814	52.68	1
20210812195450.322215	execQuery	3	0	17.17	0
20210812195451.759951	read	3	84190	1.01	1
20210812195453.486352	execQuery	4	0	1.68	0
20210812195456.287069	read	4	37313	2.62	1
20210812195545.703035	execQuery	5	0	49.37	0
20210812195627.396019	read	5	495488	39.39	1
20210812195806.999498	execQuery	6	0	99.56	0
20210812195836.750061	read	6	270910	28.50	1
20210812200034.351082	execQuery	7	0	117.56	0
20210812200119.527660	read	7	363375	43.49	1
20210812200213.956848	execQuery	8	0	54.38	0
20210812200302.197265	read	8	427245	46.25	1
20210812200302.296291	write	8	2261806	258.14	3
20210812200302.297175	totalTime	0	0	621.11	0
```

fields:
- timestamp
- type: execQuery is time of cursor.execute(), read is read, write is writes, totalTime is... well, total Time on this Log File.  
- jobID (line number on jobs.csv file)
- rows read or written
- seconds: total seconds of read or write operations. on multi thread, it is the sum of all threads.
- thread count
