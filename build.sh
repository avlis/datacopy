#!/bin/bash
# note all args are passed builds, practical for --build-arg HTTP_PROXY=...

PROXY_ARGS="--network=host"
if [ -z "${httpd_proxy}" ]; then
	PROXY_ARGS="${PROXY_ARGS} --build-arg HTTPS_PROXY=${HTTPS_PROXY}"
fi
if [ -z "${http_proxy}" ]; then
	PROXY_ARGS="${PROXY_ARGS} --build-arg HTTP_PROXY=${HTTP_PROXY}"
fi
if [ -z "${no_proxy}" ]; then
	PROXY_ARGS="${PROXY_ARGS} --build-arg NO_PROXY=${NO_PROXY}"
fi

BASEIMAGE=${BASEIMAGE:-python:3.12.8-slim}

BASENAME=${BASENAME:-datacopy}
FINALNAME=${FINALNAME:-datacopy}

BASEDOCKERFILE=${BASEDOCKERFILE:-base.Dockerfile}
FINALDOCKERFILE=${FINALDOCKERFILE:-Dockerfile}

if [ ! -z "${EXTRAVERSION}" ]; then
	EXTRAVERSION="-${EXTRAVERSION}"
fi

if [ -z "${SKIP_BUILD_BASE}" ]; then
	echo "*** cleaning up BASE [${BASENAME}] images"
	docker rm baseexport 2>&1 >/dev/null
	docker rmi ${BASENAME}:build ${BASENAME}:flat

	if [ -z "${SKIP_PULL_PYTHON}" ]; then
		echo "*** refreshing ${BASEIMAGE}"
		docker pull ${BASEIMAGE}
	fi

	echo "*** building ${BASENAME}:build"
	if 	docker build --shm-size=2G -t ${BASENAME}:build -f ${BASEDOCKERFILE} --build-arg BASEIMAGE=${BASEIMAGE} ${PROXY_ARGS} "$@" . && \
		docker run --name baseexport ${BASENAME}:build /bin/true && \
		docker export baseexport | docker import - ${BASENAME}:flat ; then
		docker rm baseexport 2>&1 >/dev/null
		echo
		echo "BASE done!"
	else
		echo
		echo "build BASE failed!"
		exit 1
	fi
fi

echo "*** building ${FINALNAME}${EXTRAVERSION}:latest"
docker rmi ${FINALNAME}${EXTRAVERSION}:latest

if [ ! -z "${EXTRAVERSION}" ]; then
	BARGS="--build-arg EXTRAVERSION=${EXTRAVERSION}"
fi
if [ ! -z "${BASENAME}" ]; then
	BARGS="${BARGS} --build-arg BASENAME=${BASENAME}"
fi

docker build -t ${FINALNAME}${EXTRAVERSION}:latest -f ${FINALDOCKERFILE} . ${BARGS} ${PROXY_ARGS} "$@"
