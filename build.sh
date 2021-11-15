#!/bin/bash
[ "$1" == "pull" ] && git pull
docker rmi localhost/datacopy
rm -f /dev/shm/datacopy.tgz
docker build --squash -t datacopy:latest . && docker save datacopy:latest -o /dev/shm/datacopy.tgz 
