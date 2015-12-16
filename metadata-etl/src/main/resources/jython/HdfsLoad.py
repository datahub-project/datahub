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

__author__ = 'zsun'
import sys
from com.ziclix.python.sql import zxJDBC
from wherehows.common import Constant


class HdfsLoad:
  def load_metadata(self):
    """
    Load dataset metadata into final table
    :return: nothing
    """
    cursor = self.conn_mysql.cursor()
    load_cmd = '''
        DELETE FROM stg_dict_dataset WHERE db_id = '{db_id}';

        LOAD DATA LOCAL INFILE '{source_file}'
        INTO TABLE stg_dict_dataset
        FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
        (`name`, `schema`, properties, fields, urn, source, sample_partition_full_path, source_created_time, source_modified_time)
        SET db_id = {db_id},
        -- TODO storage_type = 'Avro',
        wh_etl_exec_id = {wh_etl_exec_id};

        -- SHOW WARNINGS LIMIT 20;

        -- SELECT COUNT(*) FROM stg_dict_dataset;

        -- clear
        DELETE FROM stg_dict_dataset
        where db_id = {db_id}
          AND (length(`name`) = 0
           OR name like 'tmp\_%'
           OR urn like '%/\_tmp'
           OR urn like '%/\_distcp\_%')
        ;

        update stg_dict_dataset
        set location_prefix =
          case
            when source in ('Espresso', 'Oracle', 'Hdfs', 'Kafka', 'Hive')
            then substring_index(substring_index(urn, '/', 5), '/', -3) /* get the leading 2 levels */
            when urn like '%:///%/%'
            then substring_index(substring_index(urn, '/', 4), '/', -2) /* get the leading 1 level */
          end
        WHERE db_id = {db_id} and location_prefix is null;

        -- fix for some edge cases
        update stg_dict_dataset
        set name = substring_index(urn, '/', -2)
        where db_id = {db_id}
          and name in ('1.0', '2.0', '3.0', '4.0', '0.1', '0.2', '0.3', '0.4', 'dedup', '1-day', '7-day');

        -- update parent name, this depends on the data from source system
        update stg_dict_dataset
        set parent_name =
        case
        when urn like 'hdfs:///data/external/gobblin/%'
        then substring_index(substring_index(urn, '/', 7), '/', -1)
        when (urn like 'hdfs:///%data/databases/%' or urn like 'hdfs:///%data/dbchanges/%' or urn like 'hdfs:///data/external/%')
        then substring_index(substring_index(urn, '/', 5), '/', -1)
        when (urn like 'hdfs:///%data/tracking/%' or urn like 'hdfs:///data/service/%' or urn like 'hdfs:///%data/derived/%')
        then substring_index(substring_index(urn, '/', 4), '/', -1)
        else substring_index(substring_index(urn, '/', 4), '/', -1)
        end
        where db_id = {db_id} and parent_name is null
        ;
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
        '''.format(source_file=self.input_file, db_id=self.db_id, wh_etl_exec_id=self.wh_etl_exec_id)
    for state in load_cmd.split(";"):
      print state
      cursor.execute(state)
      self.conn_mysql.commit()
    cursor.close()

  def load_field(self):
    cursor = self.conn_mysql.cursor()
    load_field_cmd = '''
        DELETE FROM stg_dict_field_detail where db_id = {db_id};

        LOAD DATA LOCAL INFILE '{source_file}'
        INTO TABLE stg_dict_field_detail
        FIELDS TERMINATED BY '\Z'
        (urn, sort_id, parent_sort_id, parent_path, field_name, data_type,
         is_nullable, default_value, data_size, namespace, description)
        SET db_id = {db_id};

        -- show warnings limit 20;

        analyze table stg_dict_field_detail;

        update stg_dict_field_detail
        set description = null
        where db_id = {db_id}
           and (char_length(trim(description)) = 0
           or description in ('null', 'N/A', 'nothing', 'empty', 'none'));

        insert into field_comments (
          user_id, comment, created, comment_crc32_checksum
        )
        select 0 user_id, description, now() created, crc32(description) from
        (
          select sf.description
          from stg_dict_field_detail sf left join field_comments fc
            on sf.description = fc.comment
          where sf.description is not null
            and fc.id is null
            and sf.db_id = {db_id}
          group by 1 order by 1
        ) d;

        analyze table field_comments;

        -- delete old record if it does not exist in this load batch anymore (but have the dataset id)
        create temporary table if not exists t_deleted_fields (primary key (field_id))
          select x.field_id
            from stg_dict_field_detail s
              join dict_dataset i
                on s.urn = i.urn
                and s.db_id = {db_id}
              right join dict_field_detail x
                on i.id = x.dataset_id
                and s.field_name = x.field_name
                and s.parent_path = x.parent_path
          where s.field_name is null
            and x.dataset_id in (
                       select d.id dataset_id
                       from stg_dict_field_detail k join dict_dataset d
                         on k.urn = d.urn
                        and k.db_id = {db_id}
            )
        ; -- run time : ~2min

        delete from dict_field_detail where field_id in (select field_id from t_deleted_fields);

        -- update the old record if some thing changed
        update dict_field_detail t join
        (
          select x.field_id, s.*
          from stg_dict_field_detail s join dict_dataset d
            on s.urn = d.urn
               join dict_field_detail x
           on s.field_name = x.field_name
          and coalesce(s.parent_path, '*') = coalesce(x.parent_path, '*')
          and d.id = x.dataset_id
          where s.db_id = {db_id}
            and (x.sort_id <> s.sort_id
                or x.parent_sort_id <> s.parent_sort_id
                or x.data_type <> s.data_type
                or x.data_size <> s.data_size or (x.data_size is null) ^ (s.data_size is null)
                or x.data_precision <> s.data_precision or (x.data_precision is null) ^ (s.data_precision is null)
                or x.is_nullable <> s.is_nullable or (x.is_nullable is null) ^ (s.is_nullable is null)
                or x.is_partitioned <> s.is_partitioned or (x.is_partitioned is null) ^ (s.is_partitioned is null)
                or x.is_distributed <> s.is_distributed or (x.is_distributed is null) ^ (s.is_distributed is null)
                or x.default_value <> s.default_value or (x.default_value is null) ^ (s.default_value is null)
                or x.namespace <> s.namespace or (x.namespace is null) ^ (s.namespace is null)
            )
        ) p
          on t.field_id = p.field_id
        set t.sort_id = p.sort_id,
            t.parent_sort_id = p.parent_sort_id,
            t.data_type = p.data_type,
            t.data_size = p.data_size,
            t.data_precision = p.data_precision,
            t.is_nullable = p.is_nullable,
            t.is_partitioned = p.is_partitioned,
            t.is_distributed = p.is_distributed,
            t.default_value = p.default_value,
            t.namespace = p.namespace,
            t.modified = now()
        ;

        insert into dict_field_detail (
          dataset_id, fields_layout_id, sort_id, parent_sort_id, parent_path,
          field_name, namespace, data_type, data_size, is_nullable, default_value,
          default_comment_id, modified
        )
        select
          d.id, 0, sf.sort_id, sf.parent_sort_id, sf.parent_path,
          sf.field_name, sf.namespace, sf.data_type, sf.data_size, sf.is_nullable, sf.default_value,
          coalesce(fc.id, t.default_comment_id) fc_id, now()
        from stg_dict_field_detail sf join dict_dataset d
          on sf.urn = d.urn
             left join field_comments fc
          on sf.description = fc.comment
             left join dict_field_detail t
          on d.id = t.dataset_id
         and sf.field_name = t.field_name
         and sf.parent_path = t.parent_path
        where db_id = {db_id} and t.field_id is null

        on duplicate key update
          data_type = sf.data_type, data_size = sf.data_size,
          is_nullable = sf.is_nullable, default_value = sf.default_value,
          namespace = sf.namespace,
          default_comment_id = coalesce(fc.id, t.default_comment_id),
          modified=now()
        ;
        analyze table dict_field_detail;
        '''.format(source_file=self.input_field_file, db_id=self.db_id)
    for state in load_field_cmd.split(";"):
      print state
      cursor.execute(state)
      self.conn_mysql.commit()
    cursor.close()

  def load_sample(self):
    cursor = self.conn_mysql.cursor()
    load_sample_cmd = '''
    DELETE FROM stg_dict_dataset_sample where db_id = {db_id};

    LOAD DATA LOCAL INFILE '{source_file}'
    INTO TABLE stg_dict_dataset_sample
    FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
    (urn,ref_urn,data)
    SET db_id = {db_id};

    -- update reference id in stagging table
    UPDATE  stg_dict_dataset_sample s
    LEFT JOIN dict_dataset d ON s.ref_urn = d.urn
    SET s.ref_id = d.id
    WHERE s.db_id = {db_id};

    -- first insert ref_id as 0
    INSERT INTO dict_dataset_sample
    ( `dataset_id`,
      `urn`,
      `ref_id`,
      `data`,
      created
    )
    select d.id as dataset_id, s.urn, s.ref_id, s.data, now()
    from stg_dict_dataset_sample s left join dict_dataset d on d.urn = s.urn
          where s.db_id = {db_id}
    on duplicate key update
      `data`=s.data, modified=now();

      -- update reference id in final table
    UPDATE dict_dataset_sample d
    RIGHT JOIN stg_dict_dataset_sample s ON d.urn = s.urn
    SET d.ref_id = s.ref_id
    WHERE s.db_id = {db_id} AND d.ref_id = 0;
    '''.format(source_file=self.input_sample_file, db_id=self.db_id)
    for state in load_sample_cmd.split(";"):
      print state
      cursor.execute(state)
      self.conn_mysql.commit()
    cursor.close()


if __name__ == "__main__":
  args = sys.argv[1]

  l = HdfsLoad()

  # set up connection
  username = args[Constant.WH_DB_USERNAME_KEY]
  password = args[Constant.WH_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.WH_DB_URL_KEY]
  l.input_file = args[Constant.HDFS_SCHEMA_RESULT_KEY]
  l.input_field_file = args[Constant.HDFS_FIELD_RESULT_KEY]
  l.input_sample_file = args[Constant.HDFS_SAMPLE_LOCAL_PATH_KEY]

  l.db_id = args[Constant.DB_ID_KEY]
  l.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
  l.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
  l.load_metadata()
  l.load_field()
  l.load_sample()
  l.conn_mysql.close()
