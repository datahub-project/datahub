#!/bin/bash

set -ex

PYSPARK_JARS="$(python -c 'import site; print(site.getsitepackages()[0])')/pyspark/jars"

function replace_jar {
  JAR_PREFIX=$1
  TRANSITIVE=$2
  DEPENDENCY=$3

  echo "Removing version conflicts for $PYSPARK_JARS/$JAR_PREFIX*.jar"
  ls "$PYSPARK_JARS/$JAR_PREFIX"*.jar || true
  rm "$PYSPARK_JARS/$JAR_PREFIX"*.jar || true
  rm -r "$HOME/.m2" || true

  if [ ! -z "$DEPENDENCY" ]; then
    echo "Resolving $DEPENDENCY"
    mvn dependency:get -Dtransitive=$TRANSITIVE -Dartifact="$DEPENDENCY" >/dev/null

    echo "Moving jars to $PYSPARK_JARS"
    find "$HOME/.m2" -type f -name "$JAR_PREFIX*.jar" -exec echo "{}" \;
    find "$HOME/.m2" -type f -name "$JAR_PREFIX*.jar" -exec cp {} "$PYSPARK_JARS/" \;
  fi
}

replace_jar "zookeeper-" "false" "${ZOOKEEPER_DEPENDENCY:-org.apache.zookeeper:zookeeper:3.7.2}"
replace_jar "hadoop-client-" "true" "${HADOOP_CLIENT_API_DEPENDENCY:-org.apache.hadoop:hadoop-client-api:3.3.6}"
replace_jar "hadoop-client-" "true" "${HADOOP_CLIENT_RUNTIME_DEPENDENCY:-org.apache.hadoop:hadoop-client-runtime:3.3.6}"
replace_jar "hadoop-yarn-" "true" "${HADOOP_YARN_DEPENDENCY:-org.apache.hadoop:hadoop-yarn-server-web-proxy:3.3.6}"
replace_jar "snappy-java-" "false" "${SNAPPY_JAVA_DEPENDENCY:-org.xerial.snappy:snappy-java:1.1.10.5}"
replace_jar "libthrift-" "false" "${LIBTHRIFT_DEPENDENCY:-org.apache.thrift:libthrift:0.19.0}"
replace_jar "ivy-" "false" "${IVY_DEPENDENCY:-org.apache.ivy:ivy:2.5.2}"
replace_jar "parquet-jackson-" "false" "${PARQUET_JACKSON_DEPENDENCY:-org.apache.parquet:parquet-jackson:1.13.1}"
