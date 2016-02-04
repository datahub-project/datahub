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

from org.slf4j import LoggerFactory
from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
import sys


class OwnerTransform:
  _tables = {"source_code_commit": {"columns": "repository_urn, commit_id, file_path, file_name, commit_time, committer_name, committer_email, author_name, author_email, message",
                                    "file": "commit.csv",
                                    "table": "stg_source_code_commit_info"}
             }

  _clear_staging_tempalte = """
                            DELETE FROM {table}
                            """

  _read_file_template = """
                        LOAD DATA LOCAL INFILE '{folder}/{file}'
                        INTO TABLE {table}
                        FIELDS TERMINATED BY '\x1a' ESCAPED BY '\0'
                        LINES TERMINATED BY '\n'
                        ({columns})
                        SET app_id = {app_id},
                        wh_etl_exec_id = {wh_etl_exec_id};
                        """

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.wh_etl_exec_id = int(args[Constant.WH_EXEC_ID_KEY])
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(self.app_id)

  def run(self):
    try:
      self.read_file_to_stg()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def read_file_to_stg(self):
    t = self._tables["source_code_commit"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder,
                                            file=t.get("file"),
                                            table=t.get("table"),
                                            columns=t.get("columns"),
                                            app_id=self.app_id,
                                            wh_etl_exec_id=self.wh_etl_exec_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()


if __name__ == "__main__":
  props = sys.argv[1]
  ot = OwnerTransform(props)
  ot.run()
