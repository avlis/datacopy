#!/bin/bash
source /etc/profile.d/datacopy.sh 

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

su -p -g ${RUN_GNAME} ${RUN_UNAME} -c "/usr/local/bin/python3 /usr/local/bin/datacopy.py"