FROM spark-base

# -- Runtime

WORKDIR ${SHARED_WORKSPACE}

ENTRYPOINT sleep 30 && \
           cd python-spark-lineage-test && \
           ./python_test_run.sh $SPARK_HOME ../spark-docker.conf && \
           cd ../java-spark-lineage-test && ./java_test_run.sh $SPARK_HOME ../spark-docker.conf



