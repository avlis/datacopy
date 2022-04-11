#!/bin/bash
[ "$1" == "pull" ] && git pull
if [ "$2" == "" ]; then
        EXTRAVERSION=""
else
        EXTRAVERSION="-${2}"
fi
docker rmi datacopy${EXTRAVERSION}:latest
rm -f /dev/shm/datacopy${EXTRAVERSION}.tgz
if docker build --squash -t datacopy${EXTRAVERSION}:latest . && docker save datacopy${EXTRAVERSION}:latest -o /dev/shm/datacopy${EXTRAVERSION}.tgz; then
        echo
        echo "DONE!"
        echo "now you can use:"
        echo  "docker load < /dev/shm/datacopy${EXTRAVERSION}.tgz to update local images on other accounts."
else
        echo "build failed!"
fi
