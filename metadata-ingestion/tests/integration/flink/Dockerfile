FROM flink:1.19-scala_2.12

# JDBC connector + PostgreSQL driver: JDBC catalog for 3-level naming tests
# Kafka connector: streaming source/sink for multi-hop lineage tests
# Iceberg runtime: filesystem-based Iceberg catalog for 2-level naming tests
# Hive connector: HiveCatalog for persistent Kafka table definitions across sessions
# Hadoop uber: required by Iceberg's FlinkCatalogFactory (unshaded Hadoop classes)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.7.1/iceberg-flink-runtime-1.19-1.7.1.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.19.1/flink-sql-connector-hive-3.1.3_2.12-1.19.1.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# hive-site.xml: points Flink's HiveCatalog at the Hive Metastore thrift endpoint
RUN mkdir -p /opt/hive-conf && \
    printf '%s\n' \
      '<?xml version="1.0"?>' \
      '<configuration>' \
      '  <property>' \
      '    <name>hive.metastore.uris</name>' \
      '    <value>thrift://hive-metastore:9083</value>' \
      '  </property>' \
      '</configuration>' > /opt/hive-conf/hive-site.xml

# PyFlink: enables DataStream API job submission for integration tests.
# The Flink 1.19 image (Ubuntu 22.04) bundles PyFlink as zip files in /opt/flink/opt/python/.
# Install Python3 and set PYTHONPATH — no pip install of apache-flink needed.
# PyFlink for DataStream API job submission.
# Uses bundled pyflink.zip from /opt/flink/opt/python/ (no pip install of apache-flink).
# Only installs Python3 + minimal runtime deps that the bundled zips need.
# Install apache-flink with all deps EXCEPT pemja (needs JNI headers not in JRE).
# pemja is only needed for Python UDF execution, not for DataStream job submission.
# Step 1: install apache-flink and apache-flink-libraries without deps
# Step 2: install all runtime deps (from PyPI metadata) except pemja
# PyFlink for DataStream API job submission.
# apache-flink has a hard dep on pemja (JNI bridge) which needs real JDK headers.
# We don't need pemja (it's for Python UDF execution, not job submission).
# Strategy: install apache-flink --no-deps, then install all OTHER deps normally.
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir --no-deps apache-flink==1.19.1 && \
    pip3 install --no-cache-dir apache-flink-libraries==1.19.1 \
        "apache-beam>=2.43.0,!=2.44.0,<2.49.0" \
        "cloudpickle==2.2.0" \
        "avro-python3>=1.8.1" \
        "fastavro>=0.21.4,<1.10.0" \
        "numpy>=1.22.4" \
        "pandas>=1.3.0" \
        "pyarrow>=5.0.0" \
        "pytz>=2018.3" \
        "py4j==0.10.9.7" \
        "protobuf>=3.15.3" \
        "grpcio>=1.29.0" \
        "requests>=2.26.0" \
        "httplib2>=0.19.0" \
        "ruamel.yaml" && \
    apt-get purge -y build-essential python3-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/
