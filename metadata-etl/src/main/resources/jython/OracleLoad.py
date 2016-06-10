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

import sys
from com.ziclix.python.sql import zxJDBC
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class OracleLoad:
    def __init__(self):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    def load_metadata(self):
        cursor = self.conn_mysql.cursor()
        cursor.close()

    def load_field(self):
        cursor = self.conn_mysql.cursor()
        cursor.close()

    def load_sample(self):
        cursor = self.conn_mysql.cursor()
        cursor.close()


if __name__ == "__main__":
    args = sys.argv[1]

    l = OracleLoad()

    # set up connection
    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.WH_DB_URL_KEY]

    l.input_file = args[Constant.OR_METADATA_KEY]
    l.input_field_file = args[Constant.OR_FIELD_METADATA_KEY]
    l.input_sampledata_file = args[Constant.OR_SAMPLE_OUTPUT_KEY]

    do_sample = True # default load sample
    if Constant.OR_LOAD_SAMPLE in args:
        do_sample = bool(args[Constant.OR_LOAD_SAMPLE])
    l.db_id = args[Constant.DB_ID_KEY]
    l.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
    l.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)

    if Constant.INNODB_LOCK_WAIT_TIMEOUT in args:
        lock_wait_time = args[Constant.INNODB_LOCK_WAIT_TIMEOUT]
        l.conn_mysql.cursor().execute("SET innodb_lock_wait_timeout = %s;" % lock_wait_time)

    try:
        l.load_metadata()
        l.load_field()
        if do_sample:
            l.load_sample()
    finally:
        l.conn_mysql.close()
