#!/bin/sh

dockerize \
  java $JAVA_OPTS $JMX_OPTS -jar /datahub/datahub-upgrade/bin/datahub-upgrade.jar -u $UPGRADE_ID -a $ARG_1 -a $ARG_2 -a $ARG_3