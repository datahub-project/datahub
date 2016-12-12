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
    LINES TERMINATED BY '\n'
    (`app_id`, `wh_etl_exec_id`, `project_name`, `scm_type`, `owner_type`, `owner_name`, `create_time`,
    `num_of_repos`, `repos`, `license`, `description`)
    '''.format(source_file=self.mp_gitli_project_file, app_id=self.app_id)

    self.executeCommands(load_gitli_projects_cmd)
    self.logger.info("finish loading gitli projects from {}".format(self.mp_gitli_project_file))


  def load_product_repos(self):
    load_product_repos_cmd = '''
    DELETE FROM stg_product_repo WHERE app_id = {app_id};

    -- load into stg table
    LOAD DATA LOCAL INFILE '{source_file}'
    INTO TABLE stg_product_repo
    FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
    LINES TERMINATED BY '\n'
    (`app_id`, `wh_etl_exec_id`, `scm_repo_fullname`, `scm_type`, `repo_id`, `project`, `owner_type`, `owner_name`,
    `multiproduct_name`, `product_type`, `product_version`, `namespace`);

    -- map repo to oracle or espresso database
    UPDATE stg_product_repo r
    INNER JOIN
      (select database_type, substring_index(substring_index(scm_url, '/', 5), '/', -2) repo,
        GROUP_CONCAT(database_name SEPARATOR ', ') dataset_groups
        from stg_database_scm_map
        where scm_type = 'git' and database_type in ('espresso', 'oracle')
        group by repo, database_type) d
      ON d.repo = r.scm_repo_fullname
        AND r.app_id = {app_id}
    SET r.dataset_group = d.dataset_groups;

    -- map dali repo to dali dataset group
    UPDATE stg_product_repo
    SET dataset_group = REPLACE(substring_index(LEFT(scm_repo_fullname, LENGTH(scm_repo_fullname) - 9), '/', -1), '-', '_')
    WHERE app_id = {app_id}
      AND project IN ('dali-datasets', 'dali-base-datasets');

    UPDATE stg_product_repo
    SET dataset_group = concat(dataset_group, '_mp, ', dataset_group, '_mp_versioned')
    WHERE app_id = {app_id}
      AND project IN ('dali-datasets', 'dali-base-datasets')
    '''.format(source_file=self.product_repo_file, app_id=self.app_id)

    self.executeCommands(load_product_repos_cmd)
    self.logger.info("finish loading product repos from {}".format(self.product_repo_file))


  def load_product_repo_owners(self):
    load_product_repo_owners_cmd = '''
    DELETE FROM stg_repo_owner WHERE app_id IN ({app_id}, 300, 301);

    -- load into stg table
    LOAD DATA LOCAL INFILE '{source_file}'
    INTO TABLE stg_repo_owner
    FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
    LINES TERMINATED BY '\n'
    (`app_id`, `wh_etl_exec_id`, `scm_repo_fullname`, `scm_type`, `repo_id`, `owner_type`, `owner_name`, `sort_id`, `paths`);

    -- update dataset_group from repo
    UPDATE stg_repo_owner ro
    INNER JOIN stg_product_repo pr
      ON ro.app_id = {app_id} AND pr.app_id = {app_id}
        AND ro.scm_repo_fullname = pr.scm_repo_fullname
        AND pr.dataset_group IS NOT NULL
    SET ro.dataset_group = pr.dataset_group
    '''.format(source_file=self.product_repo_owner_file, app_id=self.app_id)

    self.executeCommands(load_product_repo_owners_cmd)
    self.logger.info("finish loading product repo owners from {}".format(self.product_repo_owner_file))


  def merge_repo_owners_into_dataset_owners(self):
    merge_repo_owners_into_dataset_owners_cmd = '''
    -- find owner app_id, 300 for USER, 301 for GROUP
    UPDATE stg_repo_owner stg
    JOIN (select app_id, user_id from dir_external_user_info) ldap
    ON stg.owner_name = ldap.user_id
    SET stg.app_id = ldap.app_id;

    UPDATE stg_repo_owner stg
    JOIN (select distinct app_id, group_id from dir_external_group_user_map) ldap
    ON stg.owner_name = ldap.group_id
    SET stg.app_id = ldap.app_id;

    -- INSERT/UPDATE into dataset_owner
    INSERT INTO dataset_owner (
    dataset_id, dataset_urn, owner_id, sort_id, namespace, app_id, owner_type, owner_sub_type, owner_id_type,
    owner_source, db_ids, is_group, is_active, source_time, created_time, wh_etl_exec_id, confirmed_by, confirmed_on
    )
    SELECT * FROM (
    SELECT ds.id, ds.urn, r.owner_name n_owner_id, r.sort_id n_sort_id,
        'urn:li:corpuser' n_namespace, r.app_id,
        'Owner' n_owner_type, r.owner_type n_owner_sub_type,
        case when r.app_id = 300 then 'USER' when r.app_id = 301 then 'GROUP' else null end n_owner_id_type,
        'SCM' n_owner_source, null db_ids,
        IF(r.app_id = 301, 'Y', 'N') is_group,
        'Y' is_active, 0 source_time, unix_timestamp(NOW()) created_time, r.wh_etl_exec_id,
        'system' confirmed_by, unix_timestamp(NOW()) confirmed_on
    FROM (SELECT id, urn, substring_index(substring_index(urn, '/', 4), '/', -1) ds_group
         FROM dict_dataset WHERE urn regexp '^(dalids|espresso|oracle)\:\/\/\/.*$') ds
      JOIN stg_repo_owner r
        ON r.owner_type in ('main', 'espresso_avsc', 'producer', 'consumer', 'global', 'public', 'private', 'database', 'root')
          AND FIND_IN_SET(ds.ds_group, r.dataset_group) > 0
    ) n
    ON DUPLICATE KEY UPDATE
    dataset_urn = n.urn,
    sort_id = COALESCE(n.n_sort_id, sort_id),
    -- the Owner_type precedence (from high to low) is: OWNER, PRODUCER, DELEGATE, STAKEHOLDER
    -- n.n_owner_type = Owner, highest priority
    owner_type = n.n_owner_type,
    owner_sub_type = COALESCE(owner_sub_type, n.n_owner_sub_type),
    owner_id_type = COALESCE(owner_id_type, n.n_owner_id_type),
    owner_source = CASE WHEN owner_source is null THEN 'SCM'
                    WHEN owner_source LIKE '%SCM%' THEN owner_source ELSE CONCAT(owner_source, ',SCM') END,
    namespace = COALESCE(namespace, n.n_namespace),
    wh_etl_exec_id = n.wh_etl_exec_id,
    modified_time = unix_timestamp(NOW()),
    confirmed_by = 'system',
    confirmed_on = unix_timestamp(NOW());

    -- reset dataset owner sort id
    UPDATE dataset_owner d
      JOIN (
        select dataset_urn, dataset_id, owner_type, owner_id, sort_id,
            @owner_rank := IF(@current_dataset_id = dataset_id, @owner_rank + 1, 0) rank,
            @current_dataset_id := dataset_id
        from dataset_owner, (select @current_dataset_id := 0, @owner_rank := 0) t
        where dataset_urn regexp '^(dalids|espresso|oracle)\:\/\/\/.*$'
        order by dataset_id asc, owner_type desc, sort_id asc, owner_id asc
      ) s
    ON d.dataset_id = s.dataset_id AND d.owner_id = s.owner_id
    SET d.sort_id = s.rank;
    '''

    self.executeCommands(merge_repo_owners_into_dataset_owners_cmd)
    self.logger.info("finish merging repo and dataset owners")


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
      self.merge_repo_owners_into_dataset_owners()
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Load Multiproduct metadata [%s -> %s]" % (str(begin), str(end)))
    finally:
      self.conn_cursor.close()
      self.conn_mysql.close()


if __name__ == "__main__":
  args = sys.argv[1]

  l = MultiproductLoad(args)
  l.run()
