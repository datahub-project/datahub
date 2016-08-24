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


class DatasetDescriptionLoad:

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def load_oracle_to_hdfs_map(self):
    cursor = self.conn_mysql.cursor()
    load_cmd = """
        INSERT IGNORE INTO cfg_object_name_map
        (
          object_type,
          object_sub_type,
          object_name,
          object_dataset_id,
          map_phrase,
          is_identical_map,
          mapped_object_type,
          mapped_object_sub_type,
          mapped_object_name,
          mapped_object_dataset_id,
          last_modified
        )
        SELECT 'Oracle' as object_type, 'Table' as object_sub_type,
        concat('/', t1.parent_name, '/', t1.name) as object_name,
        t1.id as object_dataset_id, 'derived from' as map_phrase,
        'N' as is_identical_map, 'Hdfs' as mapped_object_type,
        'Table' as mapped_object_sub_type,
        substring_index(t2.urn, 'hdfs://', -1) as mapped_object_name,
        t2.id as mapped_object_dataset_id, now() as last_modified
        FROM dict_dataset t1 JOIN dict_dataset t2 ON
        t2.urn = concat('hdfs:///data/databases/', substring_index(t1.urn, '/', -2))
        WHERE lower(t1.dataset_type) = 'oracle';
        """

    self.logger.info(load_cmd)
    cursor.execute(load_cmd)
    self.conn_mysql.commit()
    cursor.close()

  def load_kafka_to_hdfs_map(self):
    cursor = self.conn_mysql.cursor()
    load_cmd = """
        INSERT IGNORE INTO cfg_object_name_map
        (
          object_type,
          object_sub_type,
          object_name,
          object_dataset_id,
          map_phrase,
          is_identical_map,
          mapped_object_type,
          mapped_object_sub_type,
          mapped_object_name,
          mapped_object_dataset_id,
          last_modified
        )
        SELECT 'Kafka' as object_type, 'Table' as object_sub_type,
        concat('/', t1.parent_name, '/', t1.name) as object_name,
        t1.id as object_dataset_id, 'derived from' as map_phrase,
        'N' as is_identical_map, 'Hdfs' as mapped_object_type,
        'Table' as mapped_object_sub_type,
        substring_index(t2.urn, 'hdfs://', -1) as mapped_object_name,
        t2.id as mapped_object_dataset_id, now() as last_modified
        FROM dict_dataset t1 JOIN dict_dataset t2 ON
        t2.urn = concat('hdfs:///data/tracking/', substring_index(t1.urn, '/', -1))
        WHERE lower(t1.dataset_type) = 'kafka';
        """

    self.logger.info(load_cmd)
    cursor.execute(load_cmd)
    self.conn_mysql.commit()
    cursor.close()

  def load_hdfs_to_teradata_map(self):
    cursor = self.conn_mysql.cursor()
    load_cmd = """
        INSERT IGNORE INTO cfg_object_name_map
        (
          object_type,
          object_sub_type,
          object_name,
          object_dataset_id,
          map_phrase,
          is_identical_map,
          mapped_object_type,
          mapped_object_sub_type,
          mapped_object_name,
          mapped_object_dataset_id,
          last_modified
        )
        SELECT 'Hdfs' as object_type, 'Table' as object_sub_type,
        substring_index(t1.urn, 'hdfs://', -1) as object_name,
        t1.id as object_dataset_id, 'derived from' as map_phrase,
        'N' as is_identical_map, 'Teradata' as mapped_object_type,
        'Table' as mapped_object_sub_type,
        concat('/', t2.parent_name, '/', t2.name) as mapped_object_name,
        t2.id as mapped_object_dataset_id, now() as last_modified
        FROM dict_dataset t1 JOIN dict_dataset t2 ON
        t1.name = t2.name and lower(t2.urn) like 'teradata:///DWH_%' and lower(t2.source) = 'teradata'
        WHERE lower(t1.source) = 'Hdfs' and lower(t1.urn) like 'hdfs:///jobs/%';
        """

    self.logger.info(load_cmd)
    cursor.execute(load_cmd)
    self.conn_mysql.commit()
    cursor.close()

if __name__ == "__main__":
  args = sys.argv[1]

  d = DatasetDescriptionLoad()

  # set up connection
  username = args[Constant.WH_DB_USERNAME_KEY]
  password = args[Constant.WH_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.WH_DB_URL_KEY]

  d.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)

  try:
    d.load_oracle_to_hdfs_map()
    d.load_kafka_to_hdfs_map()
    d.load_hdfs_to_teradata_map()
  except Exception as e:
    d.logger.error(str(e))
  finally:
    d.conn_mysql.close()
