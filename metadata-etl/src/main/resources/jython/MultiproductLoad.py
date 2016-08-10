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
from wherehows.common import Constant
from org.slf4j import LoggerFactory
import sys, os, datetime


class MultiproductLoad:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.WH_DB_URL_KEY]
    self.mp_gitli_project_file = args[Constant.GIT_PROJECT_OUTPUT_KEY]
    self.product_repo_file = args[Constant.PRODUCT_REPO_OUTPUT_KEY]
    self.product_repo_owner_file = args[Constant.PRODUCT_REPO_OWNER_OUTPUT_KEY]

    self.app_id = args[Constant.APP_ID_KEY]
    self.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
    self.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
    self.conn_cursor = self.conn_mysql.cursor()

    if Constant.INNODB_LOCK_WAIT_TIMEOUT in args:
      lock_wait_time = args[Constant.INNODB_LOCK_WAIT_TIMEOUT]
      self.conn_cursor.execute("SET innodb_lock_wait_timeout = %s;" % lock_wait_time)

    self.logger.info("Load Multiproduct Metadata into {}, app_id {}, wh_exec_id {}"
                     .format(JDBC_URL, self.app_id, self.wh_etl_exec_id))


  def load_git_projects(self):
    load_gitli_projects_cmd = '''
    DELETE FROM stg_git_project WHERE app_id = {app_id};

    -- load into stg table
    LOAD DATA LOCAL INFILE '{source_file}'
    INTO TABLE stg_git_project
    FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
    IGNORE 1 LINES
    (`project_name`, `scm_type`, `owner_type`, `owner_name`, `create_time`, `num_of_repos`, `repos`,
    `license`, `description`)
    SET app_id = {app_id},
    wh_etl_exec_id = {wh_etl_exec_id};
    '''.format(source_file=self.mp_gitli_project_file, app_id=self.app_id, wh_etl_exec_id=self.wh_etl_exec_id)

    self.executeCommands(load_gitli_projects_cmd)
    self.logger.info("finish loading gitli projects from {}".format(self.mp_gitli_project_file))


  def load_product_repos(self):
    load_product_repos_cmd = '''
    DELETE FROM stg_product_repo WHERE app_id = {app_id};

    -- load into stg table
    LOAD DATA LOCAL INFILE '{source_file}'
    INTO TABLE stg_product_repo
    FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
    IGNORE 1 LINES
    (`scm_repo_fullname`, `scm_type`, `repo_id`, `project`, `owner_type`, `owner_name`,
    `multiproduct_name`, `product_type`, `product_version`, `namespace`)
    SET app_id = {app_id},
    wh_etl_exec_id = {wh_etl_exec_id};
    '''.format(source_file=self.product_repo_file, app_id=self.app_id, wh_etl_exec_id=self.wh_etl_exec_id)

    self.executeCommands(load_product_repos_cmd)
    self.logger.info("finish loading product repos from {}".format(self.product_repo_file))


  def load_product_repo_owners(self):
    load_product_repo_owners_cmd = '''
    DELETE FROM stg_repo_owner WHERE app_id = {app_id};

    -- load into stg table
    LOAD DATA LOCAL INFILE '{source_file}'
    INTO TABLE stg_repo_owner
    FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
    IGNORE 1 LINES
    (`scm_repo_fullname`, `scm_type`, `repo_id`, `owner_type`, `owner_name`, `paths`)
    SET app_id = {app_id},
    wh_etl_exec_id = {wh_etl_exec_id};
    '''.format(source_file=self.product_repo_owner_file, app_id=self.app_id, wh_etl_exec_id=self.wh_etl_exec_id)

    self.executeCommands(load_product_repo_owners_cmd)
    self.logger.info("finish loading product repo owners from {}".format(self.product_repo_owner_file))


  def executeCommands(self, commands):
    for cmd in commands.split(";"):
      self.logger.debug(cmd)
      self.conn_cursor.execute(cmd)
      self.conn_mysql.commit()

  def run(self):
    try:
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      self.load_git_projects()
      self.load_product_repos()
      self.load_product_repo_owners()
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Load Multiproduct metadata [%s -> %s]" % (str(begin), str(end)))
    finally:
      self.conn_cursor.close()
      self.conn_mysql.close()


if __name__ == "__main__":
  args = sys.argv[1]

  l = MultiproductLoad(args)
  l.run()
