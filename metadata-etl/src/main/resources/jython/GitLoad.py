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


class GitLoad:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.app_id = int(args[Constant.APP_ID_KEY])

  def run(self):
    try:
      self.load_from_stg()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def load_from_stg(self):
    query = """
        INSERT IGNORE INTO source_code_commit_info
        (
            app_id, repository_urn, commit_id, file_path, file_name, commit_time, committer_name, committer_email,
            author_name, author_email, message, created_time, wh_etl_exec_id
        )
        select app_id, repository_urn, commit_id, file_path, file_name, commit_time, committer_name, committer_email,
        author_name, author_email, message, unix_timestamp(NOW()), wh_etl_exec_id
        from stg_source_code_commit_info s
        where s.app_id = {app_id}
        """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()


if __name__ == "__main__":
  props = sys.argv[1]
  git = GitLoad(props)
  git.run()
