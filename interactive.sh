#!/bin/bash
docker run --rm \
--mount type=bind,source=/dev/shm,target=/dev/shm \
--mount type=bind,source=/var/run/postgresql,target=/var/run/postgresql \
--mount type=bind,source="$(pwd)",target=/app  \
-it datacopy:${IMAGETAG:-latest} /bin/bash