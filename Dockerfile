ARG EXTRAVERSION
FROM datacopy${EXTRAVERSION}:flat
ARG version="20240328-001"
ARG oracle_major_version=21
ARG oracle_minor_version=13
LABEL version=${version}
ENV VERSION=${version}
ENV LD_LIBRARY_PATH=/opt/instantclient_${oracle_major_version}_${oracle_minor_version}
WORKDIR /app
COPY datacopy.py /usr/local/bin/
COPY modules /usr/local/bin/modules/
CMD [ "/usr/local/bin/python3", "/usr/local/bin/datacopy.py" ]
