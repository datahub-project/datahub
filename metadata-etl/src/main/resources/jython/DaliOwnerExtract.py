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

from wherehows.common.writers import FileWriter
from wherehows.common import Constant
from wherehows.common.schemas import DatasetOwnerRecord


__author__ = 'zechen'

from com.ziclix.python.sql import zxJDBC
import os
import DbUtil
import sys
import re


class DaliOwnerExtract:
  def __init__(self, args):
    self.db_id = int(args[Constant.DB_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.owner_blacklist = re.split('\s*,\s*', args[Constant.GIT_COMMITTER_BLACKLIST_KEY].strip())
    print self.owner_blacklist
    self.metadata_folder = self.app_folder + "/" + str(self.db_id)

    if not os.path.exists(self.metadata_folder):
      try:
        os.makedirs(self.metadata_folder)
      except Exception as e:
        print e
    self.git_urn = args[Constant.DALI_GIT_URN_KEY]

  def run(self):
    try:
      self.collect_dali_view_owner(self.metadata_folder + "/dataset_owner.csv")
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def collect_dali_view_owner(self, file):
    # dataset_urn, owner_id, sort_id, namespace, db_name, source_time

    dali_prefix = "hive:///prod_tracking_views/"
    namespace = "urn:li:corpuser"
    db_name = "hive-nertz"
    file_writer = FileWriter(file)
    cmd = """
              select distinct file_name, email, owner_id, last_commit_time from (
                select distinct file_name, committer_email as email, trim(substring_index(committer_email, '@', 1)) owner_id, max(commit_time) last_commit_time from source_code_commit_info
                where file_name like "%.hive" and repository_urn = '{git_urn}'
                group by file_name, committer_email
                union
                select distinct file_name, author_email as email, trim(substring_index(author_email, '@', 1)) owner_id, max(commit_time) last_commit_time from source_code_commit_info
                where file_name like "%.hive" and repository_urn = '{git_urn}'
                group by file_name, author_email
              ) a where owner_id not in ({blacklist}) order by file_name, last_commit_time desc;
              """.format(git_urn=self.git_urn, blacklist=','.join('?' * len(self.owner_blacklist)))
    print cmd
    self.wh_cursor.execute(cmd, self.owner_blacklist)
    rows = DbUtil.dict_cursor(self.wh_cursor)

    prev_dataset = ""
    sort_id = 0
    for row in rows:
      dataset_urn = dali_prefix + re.split("\.", row['file_name'])[0]
      owner_id = row['owner_id']
      if dataset_urn == prev_dataset:
        sort_id += 1
      else:
        sort_id = 0
        prev_dataset = dataset_urn
      source_time = row['last_commit_time']
      dataset_owner_record = DatasetOwnerRecord(dataset_urn, owner_id, sort_id, namespace, db_name, source_time)
      file_writer.append(dataset_owner_record)

    file_writer.close()


if __name__ == "__main__":
  props = sys.argv[1]
  dali = DaliOwnerExtract(props)
  dali.run()
