FROM python:3-slim
ARG base_version="base-20240501-001"
ARG oracle_major_version=21
ARG oracle_minor_version=13
LABEL base_version=${base_version}
ENV BASE_VERSION=${base_version}
RUN echo "#!/bin/bash\nalias ll='ls -lah'\n\nexport BASE_VERSION=${BASE_VERSION}\nexport LD_LIBRARY_PATH=/opt/instantclient_${oracle_major_version}_${oracle_minor_version}" > /etc/profile.d/datacopy.sh ; chmod +x /etc/profile.d/datacopy.sh
WORKDIR /app
RUN apt-get update && apt-get -y dist-upgrade && apt-get -y install curl bash nano screen htop iftop tcpdump net-tools gnupg less iputils-ping unzip gpg procps
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && echo 'Package: *\nPin: origin packages.microsoft.com\nPin-Priority: 1\n' > /etc/apt/preferences.d/microsoft.pref
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/${oracle_major_version}${oracle_minor_version}000/instantclient-basiclite-linux.x64-${oracle_major_version}.${oracle_minor_version}.0.0.0dbru.zip > /dev/shm/oic.zip && unzip /dev/shm/oic.zip && rm -f /dev/shm/oic.zip
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/${oracle_major_version}${oracle_minor_version}000/instantclient-sqlplus-linux.x64-${oracle_major_version}.${oracle_minor_version}.0.0.0dbru.zip > /dev/shm/osp.zip && unzip /dev/shm/osp.zip && rm -f /dev/shm/osp.zip
RUN curl https://downloads.mariadb.com/MariaDB/mariadb_repo_setup > /tmp/ms.sh && chmod +x /tmp/ms.sh && /tmp/ms.sh && rm -f /tmp/ms.sh
RUN apt-get update && ACCEPT_EULA=Y apt-get -y install msodbcsql18 mssql-tools18 mariadb-client postgresql-client g++ unixodbc-dev python3-dev libpq-dev libaio1 libmariadb3 libmariadb-dev
RUN ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/ && ln -s /opt/instantclient_${oracle_major_version}_${oracle_minor_version}/sqlplus /usr/local/bin
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g;s/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf && echo -e "\n[ODBC]\nThreading = 1\n" >> /etc/odbcinst.ini
COPY pip-packages.txt pip-packages.txt
RUN pip3 install -r pip-packages.txt && rm -f pip-packages.txt
RUN pip3 cache purge && apt-get -y purge g++ unixodbc-dev python3-dev libpq-dev libmariadb-dev && apt-get -y autoremove && apt-get autoclean && apt-get clean
