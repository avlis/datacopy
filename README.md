# datacopy

data copy script, docker based, so you don't have to mess around with drivers :o)

multiprocess, so it can read fast and write faster!

reads the connections from one file (connections.csv), and queries from another.

the following env vars can be used to control it:
- CONNECTIONS_FILE: (default connections.csv)
- JOB_FILE: the csv file (default jobs.csv)
- LOG_NAME: the output files name prefix. (defaults to timestamp)
- TEST_QUERIES: set to 'yes' to only execute the select, and does not delete/write on destinations
- QUEUE_SIZE: (default 256) 
- REUSE_WRITERS: (default no)
- QUEUE_FB4NEWR: default 3, means that the buffer can be only 1/3 full before starting the next reader, if reusing writers.
- DUMP_ON_ERROR (default no)
- DUMPFILE_SEP (default | )
- STATS_IN_JSON (default no)
- SCREEN_STATS (default yes)
- SCREEN_STATS_OUTPUT (default stderr, can be set to 'stdout')
- DEBUG (default no, yes to get a lot of detail on log or stderr or both)
- DEBUG_TO_LOG (default no, sends debug messages to the log file)
- DEBUG_TO_STDERR (default no, sends debug messages to console)
- BUILD_DEBUG (default no, if yes instead of launching the python app runs /bin/bash)
- PARALLEL_READERS (default 1)
- ADD_NAMES_DELIMITERS (default no, yes to add double quotes or backticks on table and column name; useful if someone used reserved words as table names... or spaces)
- RUNAS_UID, GID: to create a regular, non privileged user to run the copy, and to create the log and stat files with the same user id and group id of a regular user on the host (instead of root). 
- IDLE_TIMEOUT_SECS: by default, datacopy will wait forever. it can be thhe case that the sources will never finish processing the query, or the destination is locked and commits don't happen. in this situations, this setting can be used to give up. NOTE: does not apply to delete/truncate stage; it just kicks in after the data copy stage. It resets every time there is an event (packet received, packet wrote, query starts, query ends, etc)
- EXECUTION_ID: when running a lot of these things, its practical. as is shows on stats and log files.
- COLLECT_MEMORY_STATS (default no, if yes will produce a new .memory log file), with memory in MB per process) 
- COLLECT_MEMORY_STATS_INTERVAL_SECS (default 1, can be used to change interval. it's a float, but anything below .2 will probably not be effective) 
- MEMORY_STATS_IN_JSON (if yes output is json like instead of csv)

- LOG_TIMESTAMP_FORMAT, STATS_TIMESTAMP_FORMAT, MEMORY_STATS_TIMESTAMP_FORMAT: you can choose between unix, float; date (regular date format, str); or compact (20250108223421.493323, dateandtime.milisecs)

See sample-run_datacopy.sh for an example of run.

See other sample-* files to have an idea of configuration.


## JOB_FILE COLUMNS (tab delimited)

- source, dest: must match something on first column of connections.csv

- mode: 
    - caps mean close log file after this one
    - t means truncate, d delete before inserting
    - i means just insert.
    - a means after, gets a max value from destination to adjust the source query. see append_column and append_query for more details. 

    examples: 
    
        - T means truncate, insert data, and close log file. 
        - t means truncate, insert, leave log file open for next query
        - i just inserts, leave log file open for next query
        - I insert and close the log file.

- query: can be a "select * from...", but if first char is @, means a file path to read the query from. or it can be just a table name, and it will build automatically a SELECT "[col1]","[col2]",(..)"[colN]" from [table].
- table: destination table name.


### Optional Columns:

- fetch_size: how many rows per read, default 1000.

- parallel_writers: how many processes are launched to process the queue and to write to the database. default 1.

- regexes: can be a placeholder/value, but if first char is @, reads placeholders values from a file, tab delimited. placeholders can be something like #DT_INI#, or anything easily searchable/replaceable on sql files. &&DT_INI is nice with oracle data sources, as the same sql statement will work on sql developer/sqlplus and will ask for replacement values.

- insert_cols: can be a list of columns to build the insert statement (comma delimited), or:
    - @: build from source query column names (default option)
    - @l: build from source, lowering case
    - @u: build from source, upping case
    - @d: build from destination (cannot be used if destination is csv)
    - anything else: comma delimited list of column names.

- ignore_cols: a list of column names, separated by comma. 

- pre_query_src, pre_query_dst: like query, but will run on source or destination connections before other statements. for things like "set dateformat ymd", for instance...

- csv_encode_special: when we have \n or \t or other control characters on data, it may mess up the csv file. 
    this option will translate special chars (ascill < 32 to \0x string representation)
    this translation is always applied to DUMP files. 

- override_insert_placeholder: to override the default %s insert placeholder. Found that some azure stuff need a ? instead...

- append_column: on mode A, with just table names on source and destination: will do a select max(append_column) on destination, and will change the source query to select * from source where append_column > max_from_dest; if the source is a custom query, the max_from_dest value will be available to be replaced with the placeholder #MAX_KEY_VALUE#.

- append_query: if for some reason, the value to be used as a bigger than filter needs a more complicated filter (or comes from a different table), you can customise the select statement. it should return only one row and one column. same rules as query applies: it can be a query on the jobs file, or it can be prefixed with an @ to point to a file.
- append_source: if for some reason it needs to come from a connection that is not the destination, it can be overriden with this column.

## includes
it is now possible to combine multiple sql files to ease management of a complex set of queries.
if the sql file has a line that starts with #include(space), that other file is read into that point of the buffer.
It's recursive, so you can build a tree of includes. It does not have any kind of recursion protection, so it it ends up on a loop... boom.

example: content of a file called sql/main_sql_file.sql:

```
#include sql/parts/file_with_select.sql
#include sql/parts/file_with_from.sql
#include sql/parts/file_with_where.sql
```

## stats
a .stats file is created on the same folder as the log file. can be a csv (tab delimited) or in json format.

example:
```
20240325100048.850060	exec_id_1	streamStart	1-o_more-d_more-kernelhistory	0	0.00	1
20240325100049.013253	exec_id_1	getMaxAtDestinationStart	1-o_more-d_more-kernelhistory	0	0.00	0
20240325100049.013883	exec_id_1	getMaxAtDestinationEnd	1-o_more-d_more-kernelhistory	288343539	0.01	0
20240325100049.069554	exec_id_1	execQueryStart	1-o_more-d_more-kernelhistory	0	0.00	1
20240325100049.072864	exec_id_1	execQueryEnd	1-o_more-d_more-kernelhistory	0	0.00	0
20240325100049.074555	exec_id_1	readDataStart	1-o_more-d_more-kernelhistory	0	0.00	1
20240325100049.206110	exec_id_1	writeDataStart	1-o_more-d_more-kernelhistory	0	0.00	1
20240325100049.210558	exec_id_1	writeDataStart	1-o_more-d_more-kernelhistory	0	0.00	2
20240325100049.215625	exec_id_1	writeDataStart	1-o_more-d_more-kernelhistory	0	0.00	3
20240325100049.216968	exec_id_1	readDataEnd	1-o_more-d_more-kernelhistory	397	0.00	0
20240325100049.234455	exec_id_1	writeDataEnd	1-o_more-d_more-kernelhistory	397	0.02	3
20240325100049.234455	exec_id_1	queueStats	1-o_more-d_more-kernelhistory	9	0.00	1000
20240325100049.235270	exec_id_1	streamEnd	1-o_more-d_more-kernelhistory	0	0.39	0
```

or json mode:
```
{"dc.ts":"20240325100128.392068","dc.execID":"exec_id_2","dc.event":"streamStart","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":1}
{"dc.ts":"20240325100128.548502","dc.execID":"exec_id_2","dc.event":"getMaxAtDestinationStart","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":0}
{"dc.ts":"20240325100128.549177","dc.execID":"exec_id_2","dc.event":"getMaxAtDestinationEnd","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":288343539,"dc.secs":0.01,"dc.threads":0}
{"dc.ts":"20240325100128.613191","dc.execID":"exec_id_2","dc.event":"execQueryStart","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":1}
{"dc.ts":"20240325100128.615363","dc.execID":"exec_id_2","dc.event":"execQueryEnd","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":0}
{"dc.ts":"20240325100128.615964","dc.execID":"exec_id_2","dc.event":"readDataStart","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":1}
{"dc.ts":"20240325100128.745757","dc.execID":"exec_id_2","dc.event":"writeDataStart","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":1}
{"dc.ts":"20240325100128.750171","dc.execID":"exec_id_2","dc.event":"writeDataStart","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":2}
{"dc.ts":"20240325100128.754842","dc.execID":"exec_id_2","dc.event":"writeDataStart","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.00,"dc.threads":3}
{"dc.ts":"20240325100128.756119","dc.execID":"exec_id_2","dc.event":"readDataEnd","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":36,"dc.secs":0.00,"dc.threads":0}
{"dc.ts":"20240325100128.765983","dc.execID":"exec_id_2","dc.event":"writeDataEnd","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":36,"dc.secs":0.01,"dc.threads":3}
{"dc.ts":"20240325100128.766758","dc.execID":"exec_id_2","dc.event":"queueStats","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":9,"dc.secs":0.00,"dc.threads":1000}
{"dc.ts":"20240325100128.766758","dc.execID":"exec_id_2","dc.event":"streamEnd","dc.jobID":"1-o_more-d_more-kernelhistory","dc.recs":0,"dc.secs":0.37,"dc.threads":0}
```

### fields:
- ts: timestamp of this event
- execID: can be used to group batches of datacopys, running as one big logical job. Useful if you run a couple of datacopys hourly, and send these stats to elasticsearch, for example.
- event: execQuery is about cursor.execute(), read is reads, write is writes, stream marks the beginings and ends of groups of jobs whitin this job file.  
- jobID (concat of line number, source, destination, destination table name, from jobs.csv file)
- recs: number of records read or written in this particular event. Only makes sense in *Ends.
- secs: total seconds of read or write operations. Only makes sense in *Ends. On multi threading writers, it is the sum of all threads (so, bigger than real time spent.). in streamEnd, it means real time seconds.
- threads: number of parallel stuff going on.

### getMaxAtDestinationEnd event:
when using the Append Mode, the "select max() from dest" value will be stored in the recs column.

### queueStats and streamEnd events:

queueStats will tell you the "high watermark" of packets in the queue. if a buffer full is observed, the secs will add up (in this particular case, not per seconds, but per events processed). This means that if secs > 0, your buffer is too small, or the number of writer threads is not enough to keep up with your readers.

In the threads column, the queueStats will store the fetch_size (records per commit), so it's easier to analise historical data for performance issues (considerations about number of threads and commit sizes).

the streamEnd used to always have the recs=0; now it shows up the total number of "idle seconds". Meaning, the amount of seconds where no events were observed. This information comes from the idleTimeout mechanism. It may be bigger than the allowed idle time, because if there some events before that limit, the idle timeout counter resets, but not this total counter.


## Signals

when SIGINT (control-C) or SIGTERM are received, an error message is written on both log and stats files, and there is an attempt to terminate the connections and threads gracefully. So use docker stop instead of docker kill if not using this interactively.


## Building the docker image:

The build is split in two parts, the "base", and the final image itself. The reason is export / import to squash the "heavy" part of the container, and allow the final image to be around 900 megabytes instead of over 1.5 GB.

if you are just updating the python code, you can run:
```
SKIP_PULL_PYTHON=yes SKIP_BUILD_BASE=yes ./build.sh
```
 