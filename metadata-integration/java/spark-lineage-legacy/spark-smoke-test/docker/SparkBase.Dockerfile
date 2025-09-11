FROM python:3.9

ARG shared_workspace=/opt/workspace


ENV SHARED_WORKSPACE=${shared_workspace}

# -- Layer: Apache Spark

ARG spark_version=3.2.0
ARG hadoop_version=2.7

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends curl gnupg && \
    curl -s https://repos.azul.com/azul-repo.key | gpg --dearmor -o /usr/share/keyrings/azul.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" | tee /etc/apt/sources.list.d/zulu.list && \
    curl https://cdn.azul.com/zulu/bin/zulu-repo_1.0.0-3_all.deb -o /tmp/zulu-repo_1.0.0-3_all.deb && \
    apt-get install /tmp/zulu-repo_1.0.0-3_all.deb && \
    apt-get update && \
#    apt-cache search zulu && \
    apt-get install -y --no-install-recommends zulu17-jre && \
    apt-get clean && \
    curl -sS https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz && \
    rm -rf /var/tmp/* /tmp/* /var/lib/apt/lists/*

RUN set -e; \
    pip install JPype1

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3.9
ENV PATH=$PATH:$SPARK_HOME/bin

COPY workspace $SHARED_WORKSPACE

WORKDIR ${SPARK_HOME}

