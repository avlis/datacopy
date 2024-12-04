ARG BASENAME=datacopy
FROM ${BASENAME}:flat
ARG version="20240707-001"
LABEL version=${version}
ENV VERSION=${version}
RUN echo "export VERSION=${VERSION}" >> /etc/profile.d/datacopy.sh
WORKDIR /app
COPY datacopy.py launch.sh /usr/local/bin/
COPY modules /usr/local/bin/modules/
ENTRYPOINT [ "/usr/local/bin/launch.sh" ]