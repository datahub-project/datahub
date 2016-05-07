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

from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
import DbUtil
import sys
from org.slf4j import LoggerFactory


class ScriptCollect():
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.look_back_days = args[Constant.AW_LINEAGE_ETL_LOOKBACK_KEY]
    self.last_execution_unix_time = None
    self.get_last_execution_unix_time()

  def get_last_execution_unix_time(self):
    if self.last_execution_unix_time is None:
      try:
        query = """
          SELECT MAX(job_finished_unixtime) as last_time FROM job_execution_data_lineage
          """
        self.wh_cursor.execute(query)
        rows = DbUtil.dict_cursor(self.wh_cursor)
        if rows:
          for row in rows:
            self.last_execution_unix_time = row['last_time']
            break
      except:
        self.logger.error("Get the last execution time from job_execution_data_lineage failed")
        self.last_execution_unix_time = None

    return self.last_execution_unix_time

  def run(self):
    sql = """
        SELECT a.application_id, a.job_id, a.attempt_number, a.script_path, a.script_type, a.script_name,
			fl.flow_name as chain_name, fj.job_name,
			CASE WHEN e.dir_short_path IS NOT NULL THEN CASE
			WHEN LOCATE('/scripts',a.script_path) > 0 THEN CONCAT('https://gitli.corp.linkedin.com',
			CONCAT(CONCAT(REPLACE(e.dir_short_path,'/code_base_','/source/code_base_'),
			SUBSTR(a.script_path,LOCATE('/scripts',a.script_path)) ),
			CONCAT('/',a.script_name) )) WHEN LOCATE('/sql',a.script_path) > 0 THEN
			CONCAT('https://gitli.corp.linkedin.com',
			CONCAT(CONCAT(REPLACE(e.dir_short_path,'/code_base_','/source/code_base_'),
			SUBSTR(a.script_path,LOCATE('/sql',a.script_path))),
			CONCAT('/',a.script_name))) END ELSE a.script_name END script_url
			FROM job_attempt_source_code a
			JOIN flow fl ON fl.app_id = a.application_id
			JOIN flow_job fj ON a.application_id = fj.app_id and fl.flow_id = fj.flow_id and a.job_id = fj.job_id
			LEFT OUTER JOIN cfg_file_system_dir_map d
			ON (d.directory_path =
			CASE WHEN LOCATE('/scripts',a.script_path) > 0 THEN
			SUBSTR(a.script_path,1,LOCATE('/scripts',a.script_path) - 1)
			WHEN LOCATE('/sql',a.script_path) > 0 THEN
			SUBSTR(a.script_path,1,LOCATE('/sql',a.script_path) - 1) END)
			LEFT OUTER JOIN cfg_scm_repository_dir e ON (d.map_to_object_id = e.scm_repo_dir_id)
        """

    git_committer = """
        SELECT git_path, commit_time, committer_name, committer_email, ui.user_id
		FROM ( SELECT CONCAT(CONCAT(CONCAT(CONCAT(repository_base_name,'/'),
		CONCAT(module_name,'/source/')),file_path), file_name) as git_path, committer_name, commit_time,
		committer_email FROM source_code_repository_info ) a
        LEFT JOIN dir_external_user_info ui on a.committer_name = ui.full_name
        WHERE a.git_path = '%s' GROUP BY committer_name
        """
    insert_sql = """
        INSERT IGNORE INTO job_execution_script (
        app_id, job_id, script_name, script_path, script_type, chain_name, job_name,
        committer_name, committer_email, committer_ldap, commit_time, script_url)
        SELECT %d, %d, '%s', '%s', '%s', '%s', '%s',
         '%s', '%s', '%s', '%s', '%s'
         FROM dual
         """
    try:
      self.logger.info('script collect')
      if self.last_execution_unix_time:
        sql += ' WHERE a.created_date >= from_unixtime(%d) - INTERVAL 1 DAY '% long(self.last_execution_unix_time)
      else:
        sql += ' WHERE a.created_date >= CURRENT_DATE - INTERVAL %d DAY '% long(self.look_back_days)
      self.logger.info(sql)
      self.wh_cursor.execute(sql)
      rows = DbUtil.copy_dict_cursor(self.wh_cursor)
      for row in rows:
        git_url = row['script_url'].replace('https://', 'git://')
        self.wh_cursor.execute(git_committer % git_url)
        git_rows = DbUtil.copy_dict_cursor(self.wh_cursor)
        if git_rows and len(git_rows) > 0:
          for git_row in git_rows:
            self.wh_cursor.execute(insert_sql %
                                 (int(row['application_id']), int(row['job_id']), row['script_name'],
                                  row['script_path'], row['script_type'], row['chain_name'], row['job_name'],
                                  git_row['committer_name'], git_row['committer_email'],
                                  git_row['user_id'] if git_row['user_id'] else git_row['committer_name'],
                                  git_row['commit_time'], row['script_url'] ))

        else:
          self.logger.info("git rows size is 0")
          self.logger.info(row['script_name'])
          self.wh_cursor.execute(insert_sql %
                               (int(row['application_id']), int(row['job_id']), row['script_name'],
                                row['script_path'], row['script_type'], row['chain_name'], row['job_name'],
                                "", "", "",
                                "", row['script_url'] ))
        self.wh_con.commit()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

if __name__ == "__main__":
  props = sys.argv[1]
  sc = ScriptCollect(props)
  sc.run()
