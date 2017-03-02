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


class EspressoTransform:

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    JDBC_DRIVER = args[Constant.WH_DB_DRIVER_KEY]
    JDBC_URL = args[Constant.WH_DB_URL_KEY]

    self.input_file = open(args[Constant.ESPRESSO_OUTPUT_KEY], 'r')

    self.db_id = args[Constant.DB_ID_KEY]
    self.wh_etl_exec_id = args[Constant.WH_EXEC_ID_KEY]
    self.conn_mysql = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
    self.conn_cursor = self.conn_mysql.cursor()

    self.logger.info("Transform ESPRESSO metadata into {}, db_id {}, wh_exec_id {}"
                     .format(JDBC_URL, self.db_id, self.wh_etl_exec_id))

    self.schema_history_cmd = "INSERT IGNORE INTO stg_dict_dataset_schema_history (urn, modified_date, dataset_schema) " + \
                              "VALUES (?, current_date - ?, ?)"

    self.dataset_cmd = "INSERT IGNORE INTO stg_dict_dataset (`db_id`, `dataset_type`, `urn`, `name`, `schema`, schema_type, " \
                       "properties, `fields`, `source`, location_prefix, parent_name, storage_type, created_time, wh_etl_exec_id) " + \
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Table', UNIX_TIMESTAMP(), ?)"

    self.owner_cmd = "INSERT IGNORE INTO stg_dataset_owner (dataset_urn, namespace, owner_id, owner_type, is_group, " \
                     "db_name, db_id, app_id, is_active, sort_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


  def convert_espresso(self, content):
    '''
    convert from original content to a insert statement
    one json could contain several tables
    one table could contain several versions
    '''
    if 'tableSchemas' in content['databaseSpec']['com.linkedin.nuage.EspressoDatabase']:
      for doc in content['databaseSpec']['com.linkedin.nuage.EspressoDatabase']['tableSchemas']:
        if 'documentSchema' not in doc:
          self.logger.info("documentSchema not exist in Espresso json [tableSchemas] section: " + json.dumps(doc))
          continue
        # different documents inside the Espresso DB
        self.construct_espresso_dataset(doc, content)

  def construct_espresso_dataset(self, doc, content):
    EXCLUDED_ATTRS_IN_PROP = ['databaseSpec', 'owners', 'parentName', 'type', 'name', 'fabric']  # need transformation

    db_name = content['name']
    doc_name = doc['name']
    urn = 'espresso:///' + db_name + '/' + doc_name
    dataset_type ='ESPRESSO'
    source = dataset_type
    location_prefix = '/' + db_name
    parent_name = db_name
    schema_type = 'JSON'
    key_list = json.loads(doc['key'])
    fields = {'fields': key_list}

    properties = {}
    for p_key in content.keys():
      if p_key not in EXCLUDED_ATTRS_IN_PROP:
        properties[p_key] = content[p_key]
    if 'dbusBootstrap' in content['databaseSpec']['com.linkedin.nuage.EspressoDatabase']:
      properties['databusBootstrap'] = content['databaseSpec']['com.linkedin.nuage.EspressoDatabase']['dbusBootstrap']
    if 'etl' in content['databaseSpec']['com.linkedin.nuage.EspressoDatabase']:
      properties['etlToHadoop'] = content['databaseSpec']['com.linkedin.nuage.EspressoDatabase']['etl']

    combined_schema = {
      'name': doc['name'],
      'doc': doc['description'],
      'keySchema': key_list,
      'valueSchema': None
    }
    # different versions of valueSchema
    pseudo_date_offset = len(doc['documentSchema'])
    for one_ver in doc['documentSchema']:
      combined_schema['valueSchema'] = json.loads(one_ver['valueSchema'])
      schema_string = json.dumps(combined_schema)
      self.conn_cursor.executemany(self.schema_history_cmd, [urn, pseudo_date_offset, schema_string])
      pseudo_date_offset -= 1

    fields['fields'].extend(combined_schema['valueSchema']['fields'])

    self.conn_cursor.executemany(self.dataset_cmd, [self.db_id, dataset_type, urn, doc_name, json.dumps(combined_schema),
                                                    schema_type, json.dumps(properties), json.dumps(fields), source,
                                                    location_prefix, parent_name, self.wh_etl_exec_id])

    owner_count = 1
    if "owners" in content:
      for owner in content['owners']:
        id_idx = owner.rfind(':')
        self.conn_cursor.executemany(self.owner_cmd, [urn, owner[:id_idx], owner[id_idx+1:], 'Delegate', 'N',
                                                      'espresso', self.db_id, 0, 'Y', owner_count])
        owner_count += 1

    if "servicesList" in content:
      for service in content['servicesList']:
        self.conn_cursor.executemany(self.owner_cmd, [urn, 'urn:li:service', service, 'Delegate', 'Y', 'espresso',
                                                      self.db_id, 0, 'Y', owner_count])
        owner_count += 1

    self.conn_mysql.commit()
    self.logger.debug('Transformed ' + urn)


  def load_espresso(self):
    for line in self.input_file:
      #print line
      one_table_info = json.loads(line)
      if len(one_table_info) > 0:
        self.convert_espresso(one_table_info)


  def clean_staging(self):
    self.conn_cursor.execute('DELETE FROM stg_dict_dataset WHERE db_id = {db_id}'.format(db_id=self.db_id))
    self.conn_cursor.execute('DELETE FROM stg_dataset_owner WHERE db_id = {db_id}'.format(db_id=self.db_id))
    self.conn_mysql.commit()


  def run(self):
    try:
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      self.clean_staging()
      self.load_espresso()
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Transform ESPRESSO metadata [%s -> %s]" % (str(begin), str(end)))
    finally:
      self.conn_cursor.close()
      self.conn_mysql.close()


if __name__ == "__main__":
  args = sys.argv[1]

  t = EspressoTransform(args)
  t.run()
