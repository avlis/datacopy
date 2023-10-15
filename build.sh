#!/bin/bash
[ "$1" == "pull" ] && git pull

if [ "$2" == "" ]; then
	EXTRAVERSION=""
else
	EXTRAVERSION="-${2}"
fi

if [ -z "${SKIPBASE}" ]; then
	docker rm dcexport 2>&1 >/dev/null
	docker rmi datacopy${EXTRAVERSION}:build datacopy${EXTRAVERSION}:flat
	if 	docker build -t datacopy${EXTRAVERSION}:build -f Dockerfile.build . && \
		docker run --name dcexport datacopy${EXTRAVERSION}:build /bin/true && \
		docker export dcexport | docker import - datacopy${EXTRAVERSION}:flat ; then
		docker rm dcexport 2>&1 >/dev/null
		echo
		echo "base done!"
	else
		echo
		echo "build BASE failed!"
		exit 1
	fi
fi
docker rmi datacopy${EXTRAVERSION}:latest
docker build -t datacopy${EXTRAVERSION}:latest -f Dockerfile .
