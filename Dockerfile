FROM harisekhon/hadoop

WORKDIR /code

RUN yum install -y wget \
    unzip

CMD "/entrypoint.sh"