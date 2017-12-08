FROM python:3.6
# -onbuild not using onbuild, because changed code results in installation time

MAINTAINER Johannes Innerbichler <j.innerbichler@gmail.com>

# test internet connection and dns settings. If apt-get update fails, restart
# docker service, check internet connection and dns settings in /etc/docker/daemon.json
RUN apt-get update

# install the official librdkafka client written in C
ENV LIBRDKAFKA_VERSION 0.11.1
RUN apt-get update && \
    git clone https://github.com/edenhill/librdkafka && cd librdkafka && \
    git checkout v${LIBRDKAFKA_VERSION} && \
    ./configure && make && make install && ldconfig

# install confluent-kafka-client
ENV CONFLUENT_KAFKA_VERSION 0.9.1.2
RUN pip install confluent-kafka==${CONFLUENT_KAFKA_VERSION}


# Copy the content of this folder into the hosts home directory.
ADD . .
RUN pip install -r requirements.txt


# setup proper configuration
ENV PYTHONPATH .

ENTRYPOINT ["python", "adapter.py"]
