#!/bin/bash

set -ex

HADOOP_CLIENT_DEPENDENCY="${HADOOP_CLIENT_DEPENDENCY:-org.apache.hadoop:hadoop-client:3.3.6}"
ZOOKEEPER_DEPENDENCY="${ZOOKEEPER_DEPENDENCY:-org.apache.zookeeper:zookeeper:3.7.2}"
PYSPARK_JARS="$(python -m site --user-site)/pyspark/jars"

# Remove conflicting versions
echo "Removing version conflicts from $PYSPARK_JARS"
CONFLICTS="zookeeper hadoop- slf4j-"
for jar in $CONFLICTS; do
  rm "$PYSPARK_JARS/$jar"*.jar
done

# Fetch dependencies
mvn dependency:get -Dtransitive=true -Dartifact="$HADOOP_CLIENT_DEPENDENCY"
mvn dependency:get -Dtransitive=true -Dartifact="$ZOOKEEPER_DEPENDENCY"

# Move to pyspark location
echo "Moving jars to $PYSPARK_JARS"
find "$HOME/.m2" -type f -name "*.jar" -exec mv {} "$PYSPARK_JARS/" \;
