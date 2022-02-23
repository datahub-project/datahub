FROM rappdw/docker-java-python:openjdk1.8.0_171-python3.6.6

ARG shared_workspace=/opt/workspace


ENV SHARED_WORKSPACE=${shared_workspace}

# -- Layer: Apache Spark

ARG spark_version=2.4.8
ARG hadoop_version=2.7

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python2.7
ENV PATH=$PATH:$SPARK_HOME/bin

COPY workspace $SHARED_WORKSPACE

WORKDIR ${SPARK_HOME}

