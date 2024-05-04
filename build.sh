#!/bin/bash
# note all args are passed to first build, practical for --build-arg HTTP_PROXY=...

if [ ! -z "${EXTRAVERSION}" ]; then
	EXTRAVERSION="-${EXTRAVERSION}"
fi

if [ -z "${SKIP_BUILD_BASE}" ]; then
	echo "*** cleaning up base images"
	docker rm dcexport 2>&1 >/dev/null
	docker rmi datacopy${EXTRAVERSION}:build datacopy${EXTRAVERSION}:flat

	if [ -z "${SKIP_FETCH_PYTHON}" ]; then
		echo "*** refreshing python:3-slim"
		docker pull python:3-slim
	fi

	echo "*** building datacopy${EXTRAVERSION}:build"
	if 	docker build -t datacopy${EXTRAVERSION}:build -f base.Dockerfile $* . && \
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
echo "*** building datacopy${EXTRAVERSION}:latest"
docker rmi datacopy${EXTRAVERSION}:latest
if [ ! -z "${EXTRAVERSION}" ]; then
	BARGS="--build-arg EXTRAVERSION=${EXTRAVERSION}"
fi
docker build -t datacopy${EXTRAVERSION}:latest -f Dockerfile . ${BARGS}
