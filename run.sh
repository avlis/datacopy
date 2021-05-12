#!/bin/bash
#Example of run file, with a local posgres, so we can use sockets.

jobname=${1/.csv/}
if [ ! -f "${jobname}.csv" ]; then
    echo "error: missing parameter, job csv file."
    exit 1
fi
[ -z "${DEBUG}" ] && _STDERR=2\>/dev/null
time eval docker run --rm --name ${jobname} --log-driver=none -a stdin -a stdout -a stderr \
--mount type=bind,source=/var/run/postgresql,target=/var/run/postgresql \
--mount type=bind,source="$(pwd)",target=/app \
-e JOB_FILE=${jobname}.csv -e LOG_FILE=${jobname} -e REUSE_WRITERS=${REUSE_WRITERS:-yes} -e QUEUE_SIZE=${QUEUE_SIZE:-2048} -e TZ=${TZ:-Europe/Lisbon} \
datacopy:latest ${_STDERR}