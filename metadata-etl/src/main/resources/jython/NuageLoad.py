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

import sys, datetime
from com.ziclix.python.sql import zxJDBC
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class NuageLoad:

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.WH_DB_URL_KEY]

    self.db_id = args[Constant.DB_ID_KEY]
    self.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
    self.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
    self.conn_cursor = self.conn_mysql.cursor()

    if Constant.INNODB_LOCK_WAIT_TIMEOUT in args:
      lock_wait_time = args[Constant.INNODB_LOCK_WAIT_TIMEOUT]
      self.conn_cursor.execute("SET innodb_lock_wait_timeout = %s;" % lock_wait_time)

    self.logger.info("Load Nuage metadata into {}, db_id {}, wh_exec_id {}"
                     .format(JDBC_URL, self.db_id, self.wh_etl_exec_id))


  def write_dataset(self):
    write_dataset_cmd = '''
    INSERT INTO dict_dataset
    ( `name`,
      `schema`,
      schema_type,
      fields,
      properties,
      urn,
      source,
      location_prefix,
      parent_name,
      storage_type,
      ref_dataset_id,
      status_id,
      dataset_type,
      hive_serdes_class,
      is_partitioned,
      partition_layout_pattern_id,
      sample_partition_full_path,
      source_created_time,
      source_modified_time,
      created_time,
      wh_etl_exec_id
      )
    SELECT s.name, s.schema, s.schema_type, s.fields,
      s.properties, s.urn,
      s.source, s.location_prefix, s.parent_name,
      s.storage_type, s.ref_dataset_id, s.status_id,
      s.dataset_type, s.hive_serdes_class, s.is_partitioned,
      s.partition_layout_pattern_id, s.sample_partition_full_path,
      s.source_created_time, s.source_modified_time, UNIX_TIMESTAMP(now()),
      s.wh_etl_exec_id
    FROM stg_dict_dataset s
    WHERE s.db_id = {db_id}
    ON DUPLICATE KEY UPDATE
      `name`=s.name, `schema`=s.schema, schema_type=s.schema_type, fields=s.fields,
      properties=s.properties, source=s.source, location_prefix=s.location_prefix, parent_name=s.parent_name,
      storage_type=s.storage_type, ref_dataset_id=s.ref_dataset_id, status_id=s.status_id,
      dataset_type=s.dataset_type, hive_serdes_class=s.hive_serdes_class, is_partitioned=s.is_partitioned,
      partition_layout_pattern_id=s.partition_layout_pattern_id, sample_partition_full_path=s.sample_partition_full_path,
      source_created_time=s.source_created_time, source_modified_time=s.source_modified_time,
      modified_time=UNIX_TIMESTAMP(now()), wh_etl_exec_id=s.wh_etl_exec_id
    '''.format(db_id=self.db_id)
    self.executeCommands(write_dataset_cmd)

  def write_owner(self):
    write_owner_cmd = '''
    -- update dataset id
    UPDATE stg_dataset_owner stg
    JOIN dict_dataset dd
      ON stg.db_id = {db_id}
      AND stg.dataset_urn = dd.urn
    SET stg.dataset_id = dd.id;

    -- update app_id
    UPDATE stg_dataset_owner stg
    JOIN dir_external_user_info ldap
      ON stg.db_id = {db_id}
      AND stg.owner_id = ldap.user_id
    SET stg.app_id = 300,
        stg.is_group = 'N',
        stg.is_active = ldap.is_active;

    UPDATE stg_dataset_owner stg
    JOIN (SELECT group_id FROM dir_external_group_user_map group by group_id) groups
      ON stg.db_id = {db_id}
      AND stg.owner_id = groups.group_id
    SET stg.app_id = 301,
        stg.is_group = 'Y',
        stg.is_active = 'Y';

    -- update owner type
    UPDATE stg_dataset_owner stg
    JOIN dir_external_user_info ldap
      ON stg.db_id = {db_id}
      AND stg.owner_id = ldap.user_id
    SET stg.owner_type = CASE WHEN ldap.department_id >= 4000 THEN 'Owner' ELSE 'Stakeholder' END,
        stg.owner_sub_type = CASE WHEN ldap.department_id = 4011 THEN 'DWH'
                                  WHEN ldap.department_id = 5526 THEN 'BA' ELSE null END;

    -- insert into owner table
    INSERT INTO dataset_owner (dataset_id, dataset_urn, owner_id, sort_id, namespace, app_id, owner_type, owner_sub_type,
        owner_id_type, owner_source, db_ids, is_group, is_active, source_time, created_time, wh_etl_exec_id)
    SELECT * FROM (
      SELECT dataset_id, dataset_urn, owner_id, sort_id n_sort_id, namespace, app_id,
        owner_type n_owner_type, owner_sub_type n_owner_sub_type,
        case when app_id = 300 then 'USER' when app_id = 301 then 'GROUP'
            when namespace = 'urn:li:service' then 'SERVICE' else null end n_owner_id_type,
        'NUAGE', db_id, is_group, is_active, source_time,
        unix_timestamp(NOW()) time_created, {wh_exec_id}
      FROM stg_dataset_owner s
      WHERE db_id = {db_id} and s.dataset_id is not null and s.owner_id > '' and app_id is not null
      ) sb
    ON DUPLICATE KEY UPDATE
    dataset_urn = sb.dataset_urn,
    sort_id = COALESCE(sort_id, sb.n_sort_id),
    owner_type = COALESCE(owner_type, sb.n_owner_type),
    owner_sub_type = COALESCE(owner_sub_type, sb.n_owner_sub_type),
    owner_id_type = COALESCE(owner_id_type, sb.n_owner_id_type),
    owner_source = CASE WHEN owner_source is null THEN 'NUAGE'
                    WHEN owner_source LIKE '%NUAGE%' THEN owner_source ELSE CONCAT(owner_source, ',NUAGE') END,
    app_id = sb.app_id,
    is_active = sb.is_active,
    db_ids = sb.db_id,
    source_time = sb.source_time,
    wh_etl_exec_id = {wh_exec_id},
    modified_time = unix_timestamp(NOW())
    '''.format(db_id=self.db_id, wh_exec_id=self.wh_etl_exec_id)
    self.executeCommands(write_owner_cmd)


  def executeCommands(self, commands):
    for cmd in commands.split(";"):
      self.logger.debug(cmd)
      self.conn_cursor.execute(cmd)
      self.conn_mysql.commit()

  def run(self):
    try:
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      self.write_dataset()
      self.write_owner()
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Load Nuage metadata db_id %s [%s -> %s]" % (self.db_id, str(begin), str(end)))
    finally:
      self.conn_cursor.close()
      self.conn_mysql.close()


if __name__ == "__main__":
  args = sys.argv[1]

  l = NuageLoad(args)
  l.run()
