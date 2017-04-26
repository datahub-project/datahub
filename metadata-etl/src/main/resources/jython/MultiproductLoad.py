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

import datetime
import os
import sys

from com.ziclix.python.sql import zxJDBC
from wherehows.common import Constant
from org.slf4j import LoggerFactory

import FileUtil


class MultiproductLoad:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.WH_DB_URL_KEY]

    temp_dir = FileUtil.etl_temp_dir(args, "MULTIPRODUCT")
    self.mp_gitli_project_file = os.path.join(temp_dir, args[Constant.GIT_PROJECT_OUTPUT_KEY])
    self.product_repo_file = os.path.join(temp_dir, args[Constant.PRODUCT_REPO_OUTPUT_KEY])
    self.product_repo_owner_file = os.path.join(temp_dir, args[Constant.PRODUCT_REPO_OWNER_OUTPUT_KEY])

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
    -- move owner info to stg_dataset_owner 
    DELETE FROM stg_dataset_owner WHERE db_id = {app_id};
    -- TODO: use app_id as db_id to differentiate in staging for now
    
    INSERT IGNORE INTO stg_dataset_owner 
    (dataset_id, dataset_urn, owner_id, namespace, owner_type, is_group, db_name, source_time,
      db_id, app_id, is_active, is_parent_urn, owner_sub_type, sort_id)
    SELECT ds.id, ds.urn, r.owner_name, 'urn:li:corpuser', 'Owner', 'N', '', null, 
        {app_id}, {app_id}, 'Y', 'N', null, r.sort_id
    FROM (SELECT id, urn, substring_index(substring_index(urn, '/', 4), '/', -1) ds_group
         FROM dict_dataset WHERE urn regexp '^(dalids|espresso|oracle)\:\/\/\/.*$') ds
      JOIN stg_repo_owner r
        ON r.owner_type in ('main', 'espresso_avsc', 'producer', 'consumer', 'global', 'public', 'private', 'database', 'root')
          AND FIND_IN_SET(ds.ds_group, r.dataset_group);

    -- update app_id
    UPDATE stg_dataset_owner stg
    JOIN dir_external_user_info ldap
      ON stg.db_id = {app_id}
      AND stg.owner_id = ldap.user_id
    SET stg.app_id = 300,
        stg.is_group = 'N',
        stg.is_active = ldap.is_active;

    UPDATE stg_dataset_owner stg
    JOIN (SELECT group_id FROM dir_external_group_user_map group by group_id) groups
      ON stg.db_id = {app_id}
      AND stg.owner_id = groups.group_id
    SET stg.app_id = 301,
        stg.is_group = 'Y',
        stg.is_active = 'Y';

    -- find deprecated dataset owner and delete
    DELETE d FROM dataset_owner d 
    JOIN
    ( SELECT o.* FROM 
      (select dataset_id, dataset_urn, app_id, owner_id from stg_dataset_owner where db_id = {app_id}) s
      RIGHT JOIN
      (select dataset_id, dataset_urn, owner_id, app_id, owner_source from dataset_owner 
        where db_ids = {app_id} and owner_source = 'SCM' and (confirmed_by is null or confirmed_by = '')
      ) o 
      ON s.dataset_id = o.dataset_id and s.dataset_urn = o.dataset_urn 
        and s.owner_id = o.owner_id and s.app_id = o.app_id
      WHERE s.owner_id is null
    ) dif
    ON d.dataset_id = dif.dataset_id and d.dataset_urn = dif.dataset_urn and d.owner_id = dif.owner_id
      and d.app_id = dif.app_id and d.owner_source <=> dif.owner_source;

    -- insert into owner table
    INSERT INTO dataset_owner (dataset_id, dataset_urn, owner_id, sort_id, namespace, app_id, owner_type, owner_sub_type,
        owner_id_type, owner_source, db_ids, is_group, is_active, source_time, created_time, wh_etl_exec_id)
    SELECT * FROM (
      SELECT dataset_id, dataset_urn, owner_id, sort_id n_sort_id, namespace n_namespace, app_id,
        owner_type n_owner_type, owner_sub_type n_owner_sub_type,
        case when app_id = 300 then 'USER' when app_id = 301 then 'GROUP'
            when namespace = 'urn:li:service' then 'SERVICE' else null end n_owner_id_type,
        'SCM', db_id, is_group, is_active, source_time,
        unix_timestamp(NOW()) time_created, {wh_etl_exec_id}
      FROM stg_dataset_owner s
      WHERE db_id = {app_id} and s.dataset_id is not null and s.owner_id > '' and app_id is not null
      ) sb
    ON DUPLICATE KEY UPDATE
    dataset_urn = sb.dataset_urn,
    sort_id = COALESCE(sort_id, sb.n_sort_id),
    owner_type = COALESCE(owner_type, sb.n_owner_type),
    owner_sub_type = COALESCE(owner_sub_type, sb.n_owner_sub_type),
    namespace = COALESCE(namespace, sb.n_namespace),
    owner_id_type = COALESCE(owner_id_type, sb.n_owner_id_type),
    app_id = sb.app_id,
    is_active = sb.is_active,
    db_ids = sb.db_id,
    source_time = sb.source_time,
    wh_etl_exec_id = {wh_etl_exec_id},
    modified_time = unix_timestamp(NOW());

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
    '''.format(app_id=self.app_id, wh_etl_exec_id=self.wh_etl_exec_id)

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
