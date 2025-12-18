# make sure you run docker build with --shm-size=2G or similar
# --- Global Scope ---
ARG PYTHON_VER=3.14
ARG BASEIMAGE=python:${PYTHON_VER}-slim
ARG base_version="base-20251218-001"


################ STAGE 1 (builder) ################
FROM ${BASEIMAGE} AS builder
ARG PYTHON_VER
ARG base_version

LABEL base_version=${base_version}

ENV PYTHON_VER=${PYTHON_VER}
ENV BASE_VERSION=${base_version}

# COMPILER TOOLS TO pip packages that need compiling
RUN apt-get update \
	&& ACCEPT_EULA=Y apt-get -y install g++ libpq-dev libmariadb-dev #python3-dev libmariadb3   

WORKDIR /app

COPY pip-packages-need-compiling.txt /app/

RUN pip3 install --upgrade pip
RUN pip3 install -r pip-packages-need-compiling.txt


################ STAGE 2  ################
FROM ${BASEIMAGE}

ARG PYTHON_VER
ARG BASEIMAGE
ARG base_version

ENV PYTHON_VER=${PYTHON_VER}
ENV BASE_VERSION=${base_version}

COPY --from=builder /usr/local/lib/python${PYTHON_VER}/site-packages /usr/local/lib/python${PYTHON_VER}/site-packages

ENV TMPDIR=/dev/shm

#ARG oracle_dl_folder=2116000
#ARG oracle_opt_folder=21_16
#ARG oracle_version=21.16.0.0.0dbru
ARG oracle_dl_folder=2326000
ARG oracle_opt_folder=23_26
ARG oracle_version=23.26.0.0.0

RUN echo "#!/bin/bash\nalias ll='ls -lah'\n\nexport BASE_VERSION=${BASE_VERSION}-$(python3 --version 2>&1 | cut -d ' ' -f 2)" > /etc/profile.d/base.sh ; chmod +x /etc/profile.d/base.sh

# BASE packages
RUN apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get -y install curl bash gnupg unzip gpg procps lsb-release mariadb-client postgresql-client libaio1t64 # libkrb5-3 krb5-user #nano screen htop iftop tcpdump net-tools less iputils-ping

# Microsoft stuff
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
	&& curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
	&& echo 'Package: *\nPin: origin packages.microsoft.com\nPin-Priority: 1\n' > /etc/apt/preferences.d/microsoft.pref
	RUN apt-get update \
	&& ACCEPT_EULA=Y apt-get -y install msodbcsql18 mssql-tools18
RUN ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g;s/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf && echo -e "\n[ODBC]\nThreading = 1\n" >> /etc/odbcinst.ini

# ORACLE: how to check for newer oracle versions: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html
RUN echo "trying to download oracle files from:"
RUN	echo "https://download.oracle.com/otn_software/linux/instantclient/${oracle_dl_folder}/instantclient-basiclite-linux.$([ "$(dpkg --print-architecture)" == "arm64" ] && echo arm64 || echo x64 )-${oracle_version}.zip"
RUN echo "https://download.oracle.com/otn_software/linux/instantclient/${oracle_dl_folder}/instantclient-sqlplus-linux.$([ "$(dpkg --print-architecture)" == "arm64" ] && echo arm64 || echo x64 )-${oracle_version}.zip"
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/${oracle_dl_folder}/instantclient-basiclite-linux.$([ "$(dpkg --print-architecture)" == "arm64" ] && echo arm64 || echo x64 )-${oracle_version}.zip > /dev/shm/oic.zip && unzip -o /dev/shm/oic.zip
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/${oracle_dl_folder}/instantclient-sqlplus-linux.$([ "$(dpkg --print-architecture)" == "arm64" ] && echo arm64 || echo x64 )-${oracle_version}.zip > /dev/shm/osp.zip && unzip -o /dev/shm/osp.zip
RUN ln -s /opt/instantclient_${oracle_opt_folder}/sqlplus /usr/local/bin
#Fix for time_t support, as currently there are no oracle drivers linked to 1t64
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ]; then GTRIPLE="aarch64"; \
    elif [ "$ARCH" = "amd64" ]; then GTRIPLE="x86_64"; \
    else GTRIPLE="$ARCH"; fi && \
    LIB_DIR="/usr/lib/${GTRIPLE}-linux-gnu" && \
    ln -s "${LIB_DIR}/libaio.so.1t64" "${LIB_DIR}/libaio.so.1"
RUN echo "export LD_LIBRARY_PATH=/opt/instantclient_${oracle_opt_folder}" >> /etc/profile.d/base.sh

# MariaDB:
RUN curl https://dlm.mariadb.com/MariaDB/mariadb_repo_setup > /tmp/ms.sh && chmod +x /tmp/ms.sh && /tmp/ms.sh && rm -f /tmp/ms.sh

#databricks stuff?
#RUN cd /opt &&  curl https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb > apache-arrow.deb && apt-get -y install ./apache-arrow.deb
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

WORKDIR /app

COPY pip-packages-whl.txt /app/

RUN pip3 install --upgrade pip
RUN pip3 install -r pip-packages-whl.txt

# CLEANUP
RUN rm -f pip-packages-*.txt; \
	pip3 cache purge; \
	apt-get -y purge g++ python3-dev libpq-dev libmariadb-dev; \
	apt-get -y autoremove; \
	apt-get autoclean; \
	apt-get clean
