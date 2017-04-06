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

import sys
from org.slf4j import LoggerFactory
from com.ziclix.python.sql import zxJDBC
from wherehows.common import Constant


class ConfidentialFieldLoad:

  confidential_field_names = ["firstname", "first_name", "lastname", "last_name",
                              "email", "email_address", "emailaddress", "address", "birthday", "birth_day",
                              "birthdate", "birth_date", "birth_month", "birthmonth", "birthyear", "birth_year", "ssn"]
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def load_metadata(self):
    cursor = self.conn_mysql.cursor()
    load_cmd = """
        INSERT IGNORE INTO dict_pii_field
        (
          field_id,
          dataset_id,
          field_name,
          modified_time
          )
          select field_id, dataset_id, field_name, UNIX_TIMESTAMP(now())
          FROM dict_field_detail WHERE lower(field_name) in
          ({fields}) GROUP BY dataset_id, field_id, field_name;
        """.format(fields="'" + "','".join(self.confidential_field_names) + "'")


    self.logger.info(load_cmd)
    cursor.execute(load_cmd)
    self.conn_mysql.commit()
    cursor.close()


if __name__ == "__main__":
  args = sys.argv[1]

  c = ConfidentialFieldLoad()

  # set up connection
  username = args[Constant.WH_DB_USERNAME_KEY]
  password = args[Constant.WH_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.WH_DB_URL_KEY]

  c.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)

  try:
    c.load_metadata()
  except Exception as e:
    c.logger.error(str(e))
  finally:
    c.conn_mysql.close()
