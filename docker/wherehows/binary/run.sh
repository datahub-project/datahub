#!/bin/bash

cd $HOME/WhereHows/backend-service
$ACTIVATOR_HOME/bin/activator start &> $HOME/logs/backend-start.log &
cd $HOME/WhereHows/web
$ACTIVATOR_HOME/bin/activator start -Dhttp.port=9008 &> $HOME/logs/web-start.log &

# make sure it runs forever.
while true; do sleep 5; done

