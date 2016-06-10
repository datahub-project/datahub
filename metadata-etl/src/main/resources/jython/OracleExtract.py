#
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

# Changed by aig on 2016-06-10

from com.ziclix.python.sql import zxJDBC
import sys, os
from wherehows.common import Constant
from org.slf4j import LoggerFactory

class OracleExtract:
    def __init__(self):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    def run(self, database_name, table_name, schema_output_file, sample_output_file, sample=True):
        """
        The entrance of the class, extract schema and sample data
        Notice the database need to have a order that the databases have more info (DWH_STG) should be scaned first.
        :param database_name:
        :param table_name:
        :param schema_output_file:
        :return:
        """
        cur = self.conn_or.cursor()
        cur.close()

if __name__ == "__main__":
    args = sys.argv[1]

    # connection
    username = args[Constant.OR_DB_USERNAME_KEY]
    password = args[Constant.OR_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.OR_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.OR_DB_URL_KEY]

    e = OracleExtract()
    e.conn_or = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
    do_sample = True
    if Constant.OR_LOAD_SAMPLE in args:
        do_sample = bool(args[Constant.OR_LOAD_SAMPLE])
    try:
        e.log_file = args[Constant.OR_LOG_KEY]
        e.databases = args[Constant.OR_TARGET_DATABASES_KEY].split(',')
        e.default_database = args[Constant.OR_DEFAULT_DATABASE_KEY]

        e.run(None, None, args[Constant.OR_SCHEMA_OUTPUT_KEY], args[Constant.OR_SAMPLE_OUTPUT_KEY], sample=do_sample)
    finally:
        e.conn_or.close()

