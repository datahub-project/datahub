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

import sys, datetime, json
from com.ziclix.python.sql import zxJDBC
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class KafkaTransform:

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.WH_DB_URL_KEY]

    self.input_file = open(args[Constant.KAFKA_OUTPUT_KEY], 'r')

    self.db_id = args[Constant.DB_ID_KEY]
    self.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
    self.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
    self.conn_cursor = self.conn_mysql.cursor()

    self.logger.info("Transform KAFKA metadata into {}, db_id {}, wh_exec_id {}"
                     .format(JDBC_URL, self.db_id, self.wh_etl_exec_id))

    self.schema_history_cmd = "INSERT IGNORE INTO stg_dict_dataset_schema_history (urn, modified_date, dataset_schema) " + \
                              "VALUES (?, current_date - ?, ?)"

    self.dataset_cmd = "INSERT IGNORE INTO stg_dict_dataset (`db_id`, `dataset_type`, `urn`, `name`, `schema`, schema_type, " \
                       "properties, `fields`, `source`, location_prefix, parent_name, storage_type, created_time, wh_etl_exec_id) " + \
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Avro', UNIX_TIMESTAMP(), ?)"

    self.owner_cmd = "INSERT IGNORE INTO stg_dataset_owner (dataset_urn, namespace, owner_id, owner_type, is_group, " \
                     "db_name, db_id, app_id, is_active, sort_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


  def convert_kafka(self, content):
    '''
    convert from original content to a insert statement
    '''
    EXCLUDED_ATTRS_IN_PROP = ['databaseSpec', 'owners', 'parentName', 'type', 'name', 'fabric', 'connectionURL', 'loadingErrors']  # need transformation
    dataset_type = 'kafka'
    name = content['name']
    parent_name = content['subType']
    urn = 'kafka:///' + name
    source = dataset_type
    location_prefix = parent_name

    # databaseSpec : topicSchemas : schema
    schema_string = ''
    properties = {}
    if 'databaseSpec' in content:
      if 'com.linkedin.nuage.KafkaTopic' in content['databaseSpec']:
        if 'topicSchemas' in content['databaseSpec']['com.linkedin.nuage.KafkaTopic']:
          versions = len(content['databaseSpec']['com.linkedin.nuage.KafkaTopic']['topicSchemas'])
          if  versions > 0:
            schema_string = content['databaseSpec']['com.linkedin.nuage.KafkaTopic']['topicSchemas'][versions - 1]['schema']

    for p_key in content.keys():
      if p_key not in EXCLUDED_ATTRS_IN_PROP:
        properties[p_key] = content[p_key]
    properties['deployments'] = content['databaseSpec']['com.linkedin.nuage.KafkaTopic']['deployments']
    properties['connectionURLs'] = content['connectionURL'].split(',')

    schema_type = 'JSON'

    fields = {}
    try:
      schema_json = json.loads(schema_string)
      fields = {'fields': schema_json['fields']}
    except ValueError:
      self.logger.debug("{} doesn't contain schema fields".format(urn))

    self.conn_cursor.executemany(self.dataset_cmd, [self.db_id, dataset_type, urn, name, json.dumps(schema_string),
                                                    schema_type, json.dumps(properties), json.dumps(fields), source,
                                                    location_prefix, parent_name, 0])

    owner_count = 1
    if "owners" in content:
      for owner in content['owners']:
        id_idx = owner.rfind(':')
        self.conn_cursor.executemany(self.owner_cmd, [urn, owner[:id_idx], owner[id_idx+1:], 'Delegate', 'N',
                                                      'kafka', self.db_id, 0, 'Y', owner_count])
        owner_count += 1

    if "servicesList" in content:
      for service in content['servicesList']:
        self.conn_cursor.executemany(self.owner_cmd, [urn, 'urn:li:service', service, 'Delegate', 'Y', 'kafka',
                                                      self.db_id, 0, 'Y', owner_count])
        owner_count += 1

    self.conn_mysql.commit()
    self.logger.debug('Transformed ' + urn)


  def load_kafka(self):
    for line in self.input_file:
      #print line
      one_table_info = json.loads(line)
      if len(one_table_info) > 0:
        self.convert_kafka(one_table_info)


  def clean_staging(self):
    self.conn_cursor.execute('DELETE FROM stg_dict_dataset WHERE db_id = {db_id}'.format(db_id=self.db_id))
    self.conn_cursor.execute('DELETE FROM stg_dataset_owner WHERE db_id = {db_id}'.format(db_id=self.db_id))
    self.conn_mysql.commit()


  def run(self):
    try:
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      self.clean_staging()
      self.load_kafka()
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Transform KAFKA metadata [%s -> %s]" % (str(begin), str(end)))
    finally:
      self.conn_cursor.close()
      self.conn_mysql.close()


if __name__ == "__main__":
  args = sys.argv[1]

  t = KafkaTransform(args)
  t.run()
