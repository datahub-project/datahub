FROM python:3.7.6-buster

# Install Java 8
RUN echo "deb http://deb.debian.org/debian/ sid main" > /etc/apt/sources.list.d/sid.list
RUN apt-get update && apt-get install -y openjdk-8-jre

# Install Spark
RUN wget http://apache.mirror.anlx.net/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz

RUN tar -xzf spark-3.0.3-bin-hadoop2.7.tgz && \
    mv spark-3.0.3-bin-hadoop2.7 /spark && \
    rm spark-3.0.3-bin-hadoop2.7.tgz

ENV SPARK_HOME="/spark"
ENV PATH="/spark/bin:$PATH"
ENV SPARK_VERSION=3.0.3
ENV SPARK_LOCAL_IP="127.0.0.1"

# Install PySpark and PyDeequ
RUN pip3 install pyspark==3.0.3
RUN pip3 install pydeequ==1.0.1
