#!/bin/bash
source /etc/profile

function getMainPid() {
    python_pid=$(ps -ef | grep "datacopy: main thread" | grep -v grep | awk '{print $2}')
    echo -n ${python_pid}
}

function waitForMainPid() {
    while true; do
        [ -z "$(getMainPid)" ] && break
        sleep 1
    done
}

function userRequestedStop() {
    python_pid=$(getMainPid)
    echo -e "\n\n*** launch.sh: control-c detected, sending it to python process id ${python_pid} ***\n\n"
    kill -s INT ${python_pid}
    #sleep 10
    waitForMainPid
    exit 15
}

function killSignalReceived() {
    python_pid=$(getMainPid)
    echo -e "\n\n*** launch.sh: kill detected, sending it to python process id ${python_pid} ***\n\n"
    kill -s TERM ${python_pid}
    #sleep 10
    waitForMainPid
    exit 15
}

trap userRequestedStop INT
trap killSignalReceived TERM

RUN_UID=${RUNAS_UID:-0}
RUN_GID=${RUNAS_GID:-0}

if [ ${RUN_GID} -eq 0 ]; then
    RUN_GNAME=root
    if [ ${RUN_UID} -eq 0 ]; then
        RUN_UNAME=root
    else
        useradd -M -g 0 -u ${RUN_UID} datacopy
        RUN_UNAME=datacopy
    fi
else
    groupadd -g ${RUN_GID} datacopy
    chgrp -R datacopy /usr/local/bin/*
    chmod -R g+rxX /usr/local/bin/*
    RUN_GNAME=datacopy
    if [ ${RUN_UID} -eq 0 ]; then
        usermod -a -G ${RUN_GID} root
        RUN_UNAME=root
    else
        useradd -M -g ${RUN_GID} -u ${RUN_UID} datacopy
        RUN_UNAME=datacopy
    fi
fi

if [ "${DEBUG}" != "yes" -a "${DEBUG_TO_LOG}" != "yes"  -a "${DEBUG_TO_STDERR}" != "yes" ]; then
    echo "*** DEBUG is not active, cleaning up code for production ***" > /dev/stderr
    sed -i 's/^(.*)logPrint\((.*)logLevel.DEBUG.*$/#DEBUG log line removed here/' /usr/local/bin/*.py /usr/local/bin/modules/*.sh
fi

#make sure proxy settings get everywhere
export http_proxy=${HTTP_PROXY}
export https_proxy=${HTTPS_PROXY}
export no_proxy=${NO_PROXY}


echo "*** datacopy container version [${BASE_VERSION}][${VERSION}] ***"
echo "*** launching with container user [${RUN_UNAME}:${RUN_GNAME}], UID=${RUN_UID}, GID=${RUN_GID} ***"
echo "*** http proxy settings http=[${http_proxy}], https=[${https_proxy}], no_proxy=[${no_proxy}] ***"
echo "*** received parameters: [$@]"

if [ "${BUILD_DEBUG}" == "yes" ]; then
    echo "BUILD_DEBUG is set, launching bash"
    /bin/bash
else
    su -p -g ${RUN_GNAME} ${RUN_UNAME} -c "/usr/local/bin/python3 /usr/local/bin/datacopy.py $1 $2 $3" &
    sudo_pid=$!
    wait ${sudo_pid}
fi
