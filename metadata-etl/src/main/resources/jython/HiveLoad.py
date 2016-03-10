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


class HiveLoad:
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def load_metadata(self):
    cursor = self.conn_mysql.cursor()
    load_cmd = """
        DELETE FROM stg_dict_dataset WHERE db_id = {db_id};

        LOAD DATA LOCAL INFILE '{source_file}'
        INTO TABLE stg_dict_dataset
        FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
        (`name`, `schema`, properties, fields, urn, source, @sample_partition_full_path, source_created_time, @source_modified_time)
        SET db_id = {db_id},
        storage_type = 'Table', dataset_type = 'hive',
        source_modified_time=nullif(@source_modified_time,''),
        sample_partition_full_path=nullif(@sample_partition_full_path,''),
        wh_etl_exec_id = {wh_etl_exec_id};

        -- SELECT COUNT(*) FROM stg_dict_dataset;
        -- clear
        DELETE FROM stg_dict_dataset where db_id = {db_id}
          AND (length(`name`)) = 0
           OR `name` like 'tmp\_%'
           OR `name` like 't\_%'
        ;

        update stg_dict_dataset
        set location_prefix = substring_index(substring_index(urn, '/', 4), '/', -2) /* hive location_prefix is it's schema name*/
        WHERE db_id = {db_id} and location_prefix is null;

        update stg_dict_dataset
        set parent_name = substring_index(substring_index(urn, '/', 4), '/', -1) /* hive parent_name is it's schema name*/
        where db_id = {db_id} and parent_name is null;

        -- insert into final table
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
        select s.name, s.schema, s.schema_type, s.fields,
          s.properties, s.urn,
          s.source, s.location_prefix, s.parent_name,
          s.storage_type, s.ref_dataset_id, s.status_id,
          s.dataset_type, s.hive_serdes_class, s.is_partitioned,
          s.partition_layout_pattern_id, s.sample_partition_full_path,
          s.source_created_time, s.source_modified_time, UNIX_TIMESTAMP(now()),
          s.wh_etl_exec_id
        from stg_dict_dataset s
        where s.db_id = {db_id}
        on duplicate key update
          `name`=s.name, `schema`=s.schema, schema_type=s.schema_type, fields=s.fields,
          properties=s.properties, source=s.source, location_prefix=s.location_prefix, parent_name=s.parent_name,
            storage_type=s.storage_type, ref_dataset_id=s.ref_dataset_id, status_id=s.status_id,
                     dataset_type=s.dataset_type, hive_serdes_class=s.hive_serdes_class, is_partitioned=s.is_partitioned,
          partition_layout_pattern_id=s.partition_layout_pattern_id, sample_partition_full_path=s.sample_partition_full_path,
          source_created_time=s.source_created_time, source_modified_time=s.source_modified_time,
            modified_time=UNIX_TIMESTAMP(now()), wh_etl_exec_id=s.wh_etl_exec_id
        ;
        analyze table dict_dataset;
        """.format(source_file=self.input_schema_file, db_id=self.db_id, wh_etl_exec_id=self.wh_etl_exec_id)

    for state in load_cmd.split(";"):
      self.logger.debug(state)
      cursor.execute(state)
      self.conn_mysql.commit()
    cursor.close()

  def load_field(self):
    """
    TODO: Load field is not used for now, as we need to open the nested structure type
    :return:
    """
    cursor = self.conn_mysql.cursor()
    load_cmd = """
        DELETE FROM stg_dict_field_detail WHERE db_id = {db_id};

        LOAD DATA LOCAL INFILE '{source_file}'
        INTO TABLE stg_dict_field_detail
        FIELDS TERMINATED BY '\Z'
        (urn, sort_id, parent_sort_id, @parent_path, field_name, field_label, data_type,
         @data_size, @precision, @scale, @is_nullable, @is_indexed, @is_partitioned, @default_value, @namespace, description,
         @dummy
        )
        SET
          parent_path=nullif(@parent_path,'null')
        , data_size=nullif(@data_size,'null')
        , data_precision=nullif(@precision,'null')
        , data_scale=nullif(@scale,'null')
        , is_nullable=nullif(@is_nullable,'null')
        , is_indexed=nullif(@is_indexed,'null')
        , is_partitioned=nullif(@is_partitioned,'null')
        , default_value=nullif(@default_value,'null')
        , namespace=nullif(@namespace,'null')
        , db_id = {db_id}
        ;


       ANALYZE TABLE stg_dict_field_detail;

       insert into dict_field_detail (
          dataset_id, fields_layout_id, sort_id, parent_sort_id, parent_path,
          field_name, namespace, data_type, data_size, is_nullable, default_value,
           modified
        )
        select
          d.id, 0, sf.sort_id, sf.parent_sort_id, sf.parent_path,
          sf.field_name, sf.namespace, sf.data_type, sf.data_size, sf.is_nullable, sf.default_value, now()
        from stg_dict_field_detail sf join dict_dataset d
          on sf.urn = d.urn
             left join dict_field_detail t
          on d.id = t.dataset_id
         and sf.field_name = t.field_name
         and sf.parent_path = t.parent_path
        where db_id = {db_id} and t.field_id is null
        ;

        analyze table dict_field_detail;


        -- delete old record in stagging
        delete from stg_dict_dataset_field_comment where db_id = {db_id};

        -- insert
        insert into stg_dict_dataset_field_comment
        select t.field_id field_id, fc.id comment_id,  d.id dataset_id, {db_id}
                from stg_dict_field_detail sf join dict_dataset d
                  on sf.urn = d.urn
                      join field_comments fc
                  on sf.description = fc.comment
                      join dict_field_detail t
                  on d.id = t.dataset_id
                 and sf.field_name = t.field_name
                 and sf.parent_path = t.parent_path
        where sf.db_id = {db_id};

        -- have default comment, insert it set default to 0
        insert ignore into dict_dataset_field_comment
        select field_id, comment_id, dataset_id, 0 is_default from stg_dict_dataset_field_comment where field_id in (
          select field_id from dict_dataset_field_comment
          where field_id in (select field_id from stg_dict_dataset_field_comment)
        and is_default = 1 ) and db_id = {db_id};


        -- doesn't have this comment before, insert into it and set as default
        insert ignore into dict_dataset_field_comment
        select sd.field_id, sd.comment_id, sd.dataset_id, 1 from stg_dict_dataset_field_comment sd
        left join dict_dataset_field_comment d
        on d.field_id = sd.field_id
         and d.comment_id = sd.comment_id
        where d.comment_id is null
        and sd.db_id = {db_id};

        """.format(source_file=self.input_field_file, db_id=self.db_id)

    # didn't load into final table for now

    for state in load_cmd.split(";"):
      self.logger.debug(state)
      cursor.execute(state)
      self.conn_mysql.commit()
    cursor.close()


if __name__ == "__main__":
  args = sys.argv[1]

  l = HiveLoad()

  # set up connection
  username = args[Constant.WH_DB_USERNAME_KEY]
  password = args[Constant.WH_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.WH_DB_URL_KEY]

  l.input_schema_file = args[Constant.HIVE_SCHEMA_CSV_FILE_KEY]
  l.input_field_file = args[Constant.HIVE_FIELD_METADATA_KEY]
  l.db_id = args[Constant.DB_ID_KEY]
  l.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
  l.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
  try:
    l.load_metadata()
    # l.load_field()
  finally:
    l.conn_mysql.close()
