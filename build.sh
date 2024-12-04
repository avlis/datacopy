#!/bin/bash
# note all args are passed builds, practical for --build-arg HTTP_PROXY=...

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

	if [ -z "${SKIP_FETCH_PYTHON}" ]; then
		echo "*** refreshing python:3-slim"
		docker pull python:3-slim
	fi

	echo "*** building ${BASENAME}:build"
	if 	docker build -t ${BASENAME}:build -f ${BASEDOCKERFILE} "$@" . && \
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

docker build -t ${FINALNAME}${EXTRAVERSION}:latest -f ${FINALDOCKERFILE} . ${BARGS} "$@"
