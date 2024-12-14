#databricks-sql-connector does not install properly on 3.13 yet
ARG BASEIMAGE=python:3.12.8-slim
FROM ${BASEIMAGE}
ARG base_version="base-20241214-001"
ARG oracle_major_version=21
ARG oracle_minor_version=16
LABEL base_version=${base_version}
ENV BASE_VERSION=${base_version}
RUN echo "#!/bin/bash\nalias ll='ls -lah'\n\nexport BASE_VERSION=${BASE_VERSION}\nexport LD_LIBRARY_PATH=/opt/instantclient_${oracle_major_version}_${oracle_minor_version}" > /etc/profile.d/datacopy.sh ; chmod +x /etc/profile.d/datacopy.sh
WORKDIR /app
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get -y dist-upgrade && apt-get -y install curl bash nano screen htop iftop tcpdump net-tools gnupg less iputils-ping unzip gpg procps libkrb5-3 krb5-user lsb-release
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && echo 'Package: *\nPin: origin packages.microsoft.com\nPin-Priority: 1\n' > /etc/apt/preferences.d/microsoft.pref
#how to check for newer oracle versions: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/${oracle_major_version}${oracle_minor_version}000/instantclient-basiclite-linux.x64-${oracle_major_version}.${oracle_minor_version}.0.0.0dbru.zip > /dev/shm/oic.zip && unzip -o /dev/shm/oic.zip && rm -f /dev/shm/oic.zip
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/${oracle_major_version}${oracle_minor_version}000/instantclient-sqlplus-linux.x64-${oracle_major_version}.${oracle_minor_version}.0.0.0dbru.zip > /dev/shm/osp.zip && unzip -o /dev/shm/osp.zip && rm -f /dev/shm/osp.zip
RUN curl https://downloads.mariadb.com/MariaDB/mariadb_repo_setup > /tmp/ms.sh && chmod +x /tmp/ms.sh && /tmp/ms.sh && rm -f /tmp/ms.sh
RUN cd /opt &&  curl https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb > apache-arrow.deb && apt-get -y install ./apache-arrow.deb
RUN apt-get update && ACCEPT_EULA=Y apt-get -y install msodbcsql18 mssql-tools18 mariadb-client postgresql-client g++ cmake unixodbc-dev python3-dev libpq-dev libaio1 libmariadb3 libmariadb-dev
RUN ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/ && ln -s /opt/instantclient_${oracle_major_version}_${oracle_minor_version}/sqlplus /usr/local/bin
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g;s/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf && echo -e "\n[ODBC]\nThreading = 1\n" >> /etc/odbcinst.ini
COPY pip-packages?.txt /app/
RUN pip3 install -r pip-packages1.txt
RUN pip3 install -r pip-packages2.txt
RUN rm -f pip-packages?.txt ; pip3 cache purge && apt-get -y purge g++ cmake unixodbc-dev python3-dev libpq-dev libmariadb-dev && apt-get -y autoremove && apt-get autoclean && apt-get clean
