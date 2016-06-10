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

from com.ziclix.python.sql import zxJDBC
import sys, os
from wherehows.common import Constant
from org.slf4j import LoggerFactory

class TeradataExtract:
    def __init__(self):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

if __name__ == "__main__":
    args = sys.argv[1]

    # connection
    username = args[Constant.TD_DB_USERNAME_KEY]
    password = args[Constant.TD_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.TD_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.TD_DB_URL_KEY]

    e = TeradataExtract()
    e.conn_td = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
    do_sample = True
    if Constant.TD_LOAD_SAMPLE in args:
        do_sample = bool(args[Constant.TD_LOAD_SAMPLE])
    try:
        e.conn_td.cursor().execute(
            "SET QUERY_BAND = 'script=%s; pid=%d; ' FOR SESSION;" % ('TeradataExtract.py', os.getpid()))
        e.conn_td.commit()
        e.log_file = args[Constant.TD_LOG_KEY]
        e.databases = args[Constant.TD_TARGET_DATABASES_KEY].split(',')
        e.default_database = args[Constant.TD_DEFAULT_DATABASE_KEY]
        index_type = {'P': 'Primary Index', 'K': 'Primary Key', 'S': 'Secondary Index', 'Q': 'Partitioned Primary Index',
                      'J': 'Join Index', 'U': 'Unique Index'}

        e.run(None, None, args[Constant.TD_SCHEMA_OUTPUT_KEY], args[Constant.TD_SAMPLE_OUTPUT_KEY], sample=do_sample)
    finally:
        e.conn_td.close()

