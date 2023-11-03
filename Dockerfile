ARG EXTRAVERSION
FROM datacopy${EXTRAVERSION}:flat
ARG version="20231103-001"
LABEL version=${version}
ENV VERSION=${version}
ENV LD_LIBRARY_PATH=/opt/instantclient_21_11
WORKDIR /app
COPY datacopy.py /usr/local/bin/
COPY modules /usr/local/bin/modules/
CMD [ "/usr/local/bin/python3", "/usr/local/bin/datacopy.py" ]
