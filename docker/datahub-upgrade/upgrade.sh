#!/bin/sh

dockerize \
  java $JAVA_OPTS $JMX_OPTS -jar /datahub/datahub-upgrade/bin/datahub-upgrade.jar $UPGRADE_AckID