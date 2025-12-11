# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

FROM spark-base

# -- Runtime

WORKDIR ${SHARED_WORKSPACE}

ENTRYPOINT sleep 30 && \
           cd python-spark-lineage-test && \
           ./python_test_run.sh $SPARK_HOME ../spark-docker.conf && \
           cd ../java-spark-lineage-test && ./java_test_run.sh $SPARK_HOME ../spark-docker.conf



