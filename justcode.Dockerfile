ARG BASENAME=datacopy
ARG version="20260207-001"

FROM ${BASENAME}:flat
ARG version
LABEL version=${version}
ENV VERSION=${version}
RUN echo "export VERSION=${VERSION}" >> /etc/profile.d/datacopy.sh ; chmod +x /etc/profile.d/datacopy.sh

COPY datacopy.py startup.sh /usr/local/bin/
COPY modules /usr/local/bin/modules/

WORKDIR /app
ENTRYPOINT [ "/usr/local/bin/startup.sh" ]
