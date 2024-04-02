#!/bin/bash

RUNAS_UID=$(id -u)
RUNAS_GID=$(id -g)

docker run --rm \
--mount type=bind,source=/dev/shm,target=/dev/shm \
--mount type=bind,source=/var/run/postgresql,target=/var/run/postgresql \
--mount type=bind,source="$(pwd)",target=/app  \
-e RUNAS_UID=${RUNAS_UID} -e RUNAS_GID=${RUNAS_GID} \
-it datacopy:${IMAGETAG:-latest} /bin/bash