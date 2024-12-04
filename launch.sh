#!/bin/bash
source /etc/profile.d/datacopy.sh

function userRequestedStop() {
    python_pid=$(ps -ef | grep "datacopy: main thread" | grep -v grep | awk '{print $2}')
    echo -e "\n\n*** launch.sh: control-c detected, sending it to python process id ${python_pid} ***\n\n"
    kill -s INT ${python_pid}
    sleep 5
}

function killSignalReceived() {
    python_pid=$(ps -ef | grep "datacopy: main thread" | grep -v grep | awk '{print $2}')
    echo -e "\n\n*** launch.sh: kill detected, sending it to python process id ${python_pid} ***\n\n"
    kill -s TERM ${python_pid}
    sleep 5
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

echo "*** datacopy container version [${BASE_VERSION}][${VERSION}] ***"
echo "*** launching with container user [${RUN_UNAME}:${RUN_GNAME}], UID=${RUN_UID}, GID=${RUN_GID} ***"
echo "*** received parameters: [$@]"

if [ -z "${BUILD_DEBUG}" ]; then
    su -p -g ${RUN_GNAME} ${RUN_UNAME} -c "/usr/local/bin/python3 /usr/local/bin/datacopy.py $@" &
    sudo_pid=$!
    wait ${sudo_pid}
else
    echo "BUILD_DEBUG is set, launching bash"
    /bin/bash
fi
