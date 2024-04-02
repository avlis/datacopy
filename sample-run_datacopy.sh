#!/bin/bash
jobname=$(basename $1 .csv)
echo "[${jobname}]"

RUNAS_UID=$(id -u)
RUNAS_GID=$(id -g)

if [ -z "$LOG_TIMESTAMP" ]; then
	LOG_TIMESTAMP="$(date +%Y%m%d_%H%M%S)_"
fi

if [ -z "${DONTDELETE}" ]; then
	RMOPT="--rm"
else
	RMOPT=""
fi

#example of a call with direct access to the host postgres socket, for higher performance 

[ -z "${DEBUG}" ] && _STDERR=2\>/dev/null
eval docker run ${RMOPT} --name ${jobname} --log-driver=none -a stdin -a stdout -a stderr --network=host \
--mount type=bind,source="$(pwd)",target=/app \
--mount type=bind,source=/var/run/postgresql,target=/var/run/postgresql \
-e RUNAS_UID=${RUNAS_UID} -e RUNAS_GID=${RUNAS_GID} \
-e JOB_FILE=${jobname}.csv -e LOG_FILE=${LOG_TIMESTAMP}-${jobname} \
-e ADD_NAMES_DELIMITERS=${ADD_NAMES_DELIMITERS:-no} \
-e REUSE_WRITERS=${REUSE_WRITERS:-yes} -e QUEUE_SIZE=${QUEUE_SIZE:-8192} -e PARALLEL_READERS=${PARALLEL_READERS:-1} \
-e TZ=${TZ:-Europe/Lisbon} -e EXECUTION_ID=${EXECUTION_ID} \
-e SCREEN_STATS=${SCREEN_STATS:-no} -e STATS_IN_JSON=${STATS_IN_JSON:-no} \
-e DEBUG=${DEBUG:-no} -e DUMP_ON_ERROR=${DUMP_ON_ERROR:-no} \
datacopy:${IMAGETAG:-latest} ${_STDERR}