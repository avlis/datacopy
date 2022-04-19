FROM python:3-slim
ARG version="20220419-001"
LABEL version=${version}
ENV VERSION=${version}
ENV LD_LIBRARY_PATH=/opt/instantclient_21_5
WORKDIR /app
COPY pip-packages.txt pip-packages.txt
RUN apt-get update && apt-get -y dist-upgrade && apt-get -y install curl bash nano screen htop iftop tcpdump net-tools gnupg less iputils-ping unzip
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && echo 'Package: *\nPin: origin packages.microsoft.com\nPin-Priority: 1\n' > /etc/apt/preferences.d/microsoft.pref
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/215000/instantclient-basiclite-linux.x64-21.5.0.0.0dbru.zip > /dev/shm/oic.zip && unzip /dev/shm/oic.zip && rm -f /dev/shm/oic.zip
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/215000/instantclient-sqlplus-linux.x64-21.5.0.0.0dbru.zip > /dev/shm/c.zip && unzip /dev/shm/c.zip && rm -f /dev/shm/c.zip
RUN apt-get update && ACCEPT_EULA=Y apt-get -y install msodbcsql18 mssql-tools18 mariadb-client postgresql-client g++ unixodbc-dev python3-dev libpq-dev libmariadb-dev libaio1
RUN pip3 install -r pip-packages.txt
RUN ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/ && ln -s /opt/instantclient_21_5/sqlplus /usr/local/bin
RUN pip3 cache purge && apt-get -y purge g++ unixodbc-dev python3-dev libpq-dev libmariadb-dev && apt-get -y autoremove && apt-get autoclean && apt-get clean
COPY datacopy.py /usr/local/bin/datacopy.py
CMD [ "/usr/local/bin/python3", "/usr/local/bin/datacopy.py" ]