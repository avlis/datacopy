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
# http proxy settings are only used for databricks stuff.

[ -z "${DEBUG}" ] && _STDERR=2\>/dev/null
eval docker run ${RMOPT} --name ${jobname} --log-driver=none -a stdin -a stdout -a stderr --network=host \
--mount type=bind,source="$(pwd)",target=/app \
--mount type=bind,source=/var/run/postgresql,target=/var/run/postgresql \
-e IDLE_TIMEOUT_SECS=${IDLE_TIMEOUT_SECS:-0} \
-e DUMP_ON_ERROR=${DUMP_ON_ERROR:-no} \
-e RUNAS_UID=${RUNAS_UID} -e RUNAS_GID=${RUNAS_GID} \
-e JOB_FILE=${jobname}.csv -e LOG_NAME=${LOG_TIMESTAMP}-${jobname} \
-e ADD_NAMES_DELIMITERS=${ADD_NAMES_DELIMITERS:-no} \
-e QUEUE_SIZE=${QUEUE_SIZE:-8192} -e PARALLEL_READERS=${PARALLEL_READERS:-1} \
-e TZ=${TZ:-Europe/Lisbon} -e EXECUTION_ID=${EXECUTION_ID} \
-e HTTP_PROXY=${http_proxy} -e HTTPS_PROXY=${https_proxy} -e NO_PROXY=${no_proxy} \
-e SCREEN_STATS=${SCREEN_STATS:-no} -e STATS_IN_JSON=${STATS_IN_JSON:-no} \
-e COLLECT_MEMORY_STATS=${COLLECT_MEMORY_STATS:-no} -e MEMORY_STATS_IN_JSON=${MEMORY_STATS_IN_JSON:-no} -e COLLECT_MEMORY_STATS_INTERVAL_SECS=${COLLECT_MEMORY_STATS_INTERVAL_SECS:-1} \
-e LOG_TIMESTAMP_FORMAT=${LOG_TIMESTAMP_FORMAT:-date} -e STATS_TIMESTAMP_FORMAT=${STATS_TIMESTAMP_FORMAT:-compact} -e MEMORY_STATS_TIMESTAMP_FORMAT=${MEMORY_STATS_TIMESTAMP_FORMAT:-linux} \
-e -e BUILD_DEBUG=${BUILD_DEBUG:-no} DEBUG=${DEBUG:-no} -e DEBUG_TO_LOG=${DEBUG_TO_LOG:-no} -e DEBUG_TO_STDERR=${DEBUG_TO_STDERR:-no} \
datacopy:${IMAGETAG:-latest} ${_STDERR}