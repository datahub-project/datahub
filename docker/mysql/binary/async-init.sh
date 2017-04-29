#!/bin/bash

# Wait for the server to be completely up before initializing it.
while [ -z "$(mysqladmin --password=$MYSQL_ROOT_PASSWORD ping 2> /dev/null | grep 'alive')" ]; do sleep 1; done

# must be in the right directory so the create tables script finds the files it sources.
cd /usr/local/wherehows

mysql --password=$MYSQL_ROOT_PASSWORD < /usr/local/wherehows/init.sql &> /var/log/mysql/create-accounts.log
mysql --password=$MYSQL_ROOT_PASSWORD -hlocalhost -uwherehows -Dwherehows < /usr/local/wherehows/create_all_tables_wrapper.sql &> /var/log/mysql/create-tables.log