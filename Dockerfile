FROM python:3-slim
ARG version="20221112-001"
LABEL version=${version}
ENV VERSION=${version}
ENV LD_LIBRARY_PATH=/opt/instantclient_21_8
WORKDIR /app
RUN apt-get update && apt-get -y dist-upgrade && apt-get -y install curl bash nano screen htop iftop tcpdump net-tools gnupg less iputils-ping unzip
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && echo 'Package: *\nPin: origin packages.microsoft.com\nPin-Priority: 1\n' > /etc/apt/preferences.d/microsoft.pref
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-basiclite-linux.x64-21.8.0.0.0dbru.zip > /dev/shm/oic.zip && unzip /dev/shm/oic.zip && rm -f /dev/shm/oic.zip
RUN curl https://downloads.mariadb.com/MariaDB/mariadb_repo_setup > /tmp/ms.sh && chmod +x /tmp/ms.sh && /tmp/ms.sh
RUN apt-get update && ACCEPT_EULA=Y apt-get -y install msodbcsql18 mssql-tools18 mariadb-client postgresql-client g++ unixodbc-dev python3-dev libpq-dev libaio1 libmariadb3 libmariadb-dev
RUN ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/ && ln -s /opt/instantclient_21_8/sqlplus /usr/local/bin
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g;s/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf && echo -e "\n[ODBC]\nThreading = 1\n" >> /etc/odbcinst.ini
COPY pip-packages.txt pip-packages.txt
RUN pip3 install -r pip-packages.txt
RUN pip3 cache purge && apt-get -y purge g++ unixodbc-dev python3-dev libpq-dev libmariadb-dev && apt-get -y autoremove && apt-get autoclean && apt-get clean
COPY datacopy.py /usr/local/bin/
COPY modules /usr/local/bin/modules/
CMD [ "/usr/local/bin/python3", "/usr/local/bin/datacopy.py" ]
