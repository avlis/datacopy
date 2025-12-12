#!/bin/bash
docker run --rm \
--mount type=bind,source=/dev/shm,target=/dev/shm \
--mount type=bind,source=/var/run/postgresql,target=/var/run/postgresql \
--mount type=bind,source="$(pwd)",target=/app  \
-e BUILD_DEBUG=yes \
-e DEBUG=${DEBUG:-no} -e DEBUG_TO_LOG=${DEBUG_TO_LOG:-no} -e DEBUG_TO_STDERR=${DEBUG_TO_STDERR:-no} \
-it ${IMAGENAME:-datacopy}:${IMAGETAG:-latest}
