FROM python:3-slim
ARG version="20210808-003"
LABEL version=${version}
ENV VERSION=${version}
ENV LD_LIBRARY_PATH=/opt/instantclient_21_1
WORKDIR /app
COPY pip-packages.txt pip-packages.txt
RUN apt-get update && apt-get -y dist-upgrade && apt-get -y install curl bash nano screen htop iftop tcpdump net-tools gnupg less iputils-ping unzip
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN cd /opt && curl https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basiclite-linux.x64-21.1.0.0.0.zip > /dev/shm/oic.zip && unzip /dev/shm/oic.zip && rm -f /dev/shm/oic.zip
RUN cd /opt &&  curl https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-sqlplus-linux.x64-21.1.0.0.0.zip > /dev/shm/c.zip && unzip /dev/shm/c.zip && rm -f /dev/shm/c.zip
RUN apt-get update && ACCEPT_EULA=Y apt-get -y install msodbcsql17 mssql-tools mariadb-client postgresql-client g++ unixodbc-dev python3-dev libpq-dev libmariadbclient-dev
RUN pip3 install -r pip-packages.txt
RUN ln -s /opt/mssql-tools/bin/sqlcmd /usr/local/bin/ && ln -s /opt/instantclient_21_1/sqlplus /usr/local/bin
RUN pip3 cache purge && apt-get -y purge g++ unixodbc-dev python3-dev libpq-dev libmariadbclient-dev && apt-get -y autoremove && apt-get autoclean && apt-get clean
COPY datacopy.py /usr/local/bin/datacopy.py
CMD [ "/usr/local/bin/python3", "/usr/local/bin/datacopy.py" ]