#!/bin/bash

cd .. && ./datahub-upgrade.sh -u NoCodeDataMigration -a batchSize=1000 -a batchDelayMs=100
