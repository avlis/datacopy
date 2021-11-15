#!/bin/bash
[ "$1" == "pull" ] && git pull
if [ "$2" == "" ]; then
        EXTRAVERSION=""
else
        EXTRAVERSION="-${2}"
fi
docker rmi localhost/datacopy
rm -f /dev/shm/datacopy.tgz
docker build --squash -t datacopy:latest . && docker save datacopy${EXTRAVERSION}:latest -o /dev/shm/datacopy.tgz 
