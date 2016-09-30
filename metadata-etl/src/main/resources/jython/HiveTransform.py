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

import json
import datetime
import sys, os
import time
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import DatasetSchemaRecord, DatasetFieldRecord, HiveDependencyInstanceRecord, DatasetInstanceRecord
from wherehows.common import Constant
from HiveExtract import TableInfo
from org.apache.hadoop.hive.ql.tools import LineageInfo
from metadata.etl.dataset.hive import HiveViewDependency

from HiveColumnParser import HiveColumnParser
from AvroColumnParser import AvroColumnParser

class HiveTransform:
  dataset_dict = {}
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    username = args[Constant.HIVE_METASTORE_USERNAME]
    password = args[Constant.HIVE_METASTORE_PASSWORD]
    jdbc_driver = args[Constant.HIVE_METASTORE_JDBC_DRIVER]
    jdbc_url = args[Constant.HIVE_METASTORE_JDBC_URL]
    self.conn_hms = zxJDBC.connect(jdbc_url, username, password, jdbc_driver)
    self.curs = self.conn_hms.cursor()
    dependency_instance_file = args[Constant.HIVE_DEPENDENCY_CSV_FILE_KEY]
    self.instance_writer = FileWriter(dependency_instance_file)

  def transform(self, input, hive_instance, hive_metadata, hive_field_metadata):
    """
    convert from json to csv
    :param input: input json file
    :param hive_metadata: output data file for hive table metadata
    :param hive_field_metadata: output data file for hive field metadata
    :return:
    """
    f_json = open(input)
    all_data = json.load(f_json)
    f_json.close()

    dataset_idx = -1

    instance_file_writer = FileWriter(hive_instance)
    schema_file_writer = FileWriter(hive_metadata)
    field_file_writer = FileWriter(hive_field_metadata)

    lineageInfo = LineageInfo()
    depends_sql = """
      SELECT d.NAME DB_NAME, case when t.TBL_NAME regexp '_[0-9]+_[0-9]+_[0-9]+$'
          then concat(substring(t.TBL_NAME, 1, length(t.TBL_NAME) - length(substring_index(t.TBL_NAME, '_', -3)) - 1),'_{version}')
        else t.TBL_NAME
        end dataset_name,
        concat('/', d.NAME, '/', t.TBL_NAME) object_name,
        case when (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and d.NAME not like 'dalitest%' and t.TBL_TYPE = 'VIRTUAL_VIEW'
          then 'dalids'
        else 'hive'
        end object_type,
        case when (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and d.NAME not like 'dalitest%' and t.TBL_TYPE = 'VIRTUAL_VIEW'
          then 'View'
        else
            case when LOCATE('view', LOWER(t.TBL_TYPE)) > 0 then 'View'
          when LOCATE('index', LOWER(t.TBL_TYPE)) > 0 then 'Index'
            else 'Table'
          end
        end object_sub_type,
        case when (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and t.TBL_TYPE = 'VIRTUAL_VIEW'
          then 'dalids'
        else 'hive'
        end prefix
      FROM TBLS t JOIN DBS d on t.DB_ID = d.DB_ID
      WHERE d.NAME = '{db_name}' and t.TBL_NAME = '{table_name}'
      """

    # one db info : 'type', 'database', 'tables'
    # one table info : required : 'name' , 'type', 'serializationFormat' ,'createTime', 'DB_ID', 'TBL_ID', 'SD_ID'
    #                  optional : 'schemaLiteral', 'schemaUrl', 'fieldDelimiter', 'fieldList'
    for one_db_info in all_data:
      i = 0
      for table in one_db_info['tables']:
        i += 1
        schema_json = {}
        prop_json = {}  # set the prop json

        for prop_name in TableInfo.optional_prop:
          if prop_name in table and table[prop_name] is not None:
            prop_json[prop_name] = table[prop_name]

        view_expanded_text = ''

        if TableInfo.view_expended_text in prop_json:
          view_expanded_text = prop_json[TableInfo.view_expended_text]
          text = prop_json[TableInfo.view_expended_text].replace('`', '')	# this will be fixed after switching to Hive AST
          array = []
          try:
            array = HiveViewDependency.getViewDependency(text)
          except:
            self.logger.error("HiveViewDependency.getViewDependency(%s) failed!" % (table['name']))

          l = []
          for a in array:
            l.append(a)
            names = str(a).split('.')
            if names and len(names) >= 2:
              db_name = names[0].lower()
              table_name = names[1].lower()
              if db_name and table_name:
                rows = []
                self.curs.execute(depends_sql.format(db_name=db_name, table_name=table_name, version='{version}'))
                rows = self.curs.fetchall()
                if rows and len(rows) > 0:
                  for row_index, row_value in enumerate(rows):
                    dependent_record = HiveDependencyInstanceRecord(
                                          one_db_info['type'],
                                          table['type'],
                                          "/%s/%s" % (one_db_info['database'], table['name']),
                                          'dalids:///' + one_db_info['database'] + '/' + table['dataset_name']
                                          if one_db_info['type'].lower() == 'dalids'
                                          else 'hive:///' + one_db_info['database'] + '/' + table['dataset_name'],
                                          'depends on',
                                          'Y',
                                          row_value[3],
                                          row_value[4],
                                          row_value[2],
                                          row_value[5] + ':///' + row_value[0] + '/' + row_value[1], '')
                    self.instance_writer.append(dependent_record)
          prop_json['view_depends_on'] = l
          self.instance_writer.flush()

        # process either schema
        flds = {}
        field_detail_list = []

        if TableInfo.schema_literal in table and \
           table[TableInfo.schema_literal] is not None and \
           table[TableInfo.schema_literal].startswith('{'):
          sort_id = 0
          urn = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          self.logger.info("Getting schema literal for: %s" % (urn))
          try:
            schema_data = json.loads(table[TableInfo.schema_literal])
            schema_json = schema_data
            acp = AvroColumnParser(schema_data, urn = urn)
            result = acp.get_column_list_result()
            field_detail_list += result
          except ValueError:
            self.logger.error("Schema Literal JSON error for table: " + str(table))

        elif TableInfo.field_list in table:
          # Convert to avro
          uri = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          if one_db_info['type'].lower() == 'dalids':
            uri = "dalids:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          else:
            uri = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])
          self.logger.info("Getting column definition for: %s" % (uri))
          try:
            hcp = HiveColumnParser(table, urn = uri)
            schema_json = {'fields' : hcp.column_type_dict['fields'], 'type' : 'record', 'name' : table['name'], 'uri' : uri}
            field_detail_list += hcp.column_type_list
          except:
            self.logger.error("HiveColumnParser(%s) failed!" % (uri))
            schema_json = {'fields' : {}, 'type' : 'record', 'name' : table['name'], 'uri' : uri}

        if one_db_info['type'].lower() == 'dalids':
          dataset_urn = "dalids:///%s/%s" % (one_db_info['database'], table['dataset_name'])
        else:
          dataset_urn = "hive:///%s/%s" % (one_db_info['database'], table['dataset_name'])

        dataset_instance_record = DatasetInstanceRecord('dalids:///' + one_db_info['database'] + '/' + table['name']
                                                if one_db_info['type'].lower() == 'dalids'
                                                else 'hive:///' + one_db_info['database'] + '/' + table['name'],
                                                'grid',
                                                '',
                                                '',
                                                '*',
                                                0,
                                                table['native_name'],
                                                table['logical_name'],
                                                table['version'],
                                                table['create_time'],
                                                json.dumps(schema_json),
                                                json.dumps(view_expanded_text),
                                                dataset_urn)
        instance_file_writer.append(dataset_instance_record)

        if dataset_urn not in self.dataset_dict:
          dataset_scehma_record = DatasetSchemaRecord(table['dataset_name'], json.dumps(schema_json), json.dumps(prop_json),
                                                    json.dumps(flds),
                                                    dataset_urn,
                                                    'Hive', one_db_info['type'], table['type'],
                                                    '', (table[TableInfo.create_time] if table.has_key(
            TableInfo.create_time) else None), (table["lastAlterTime"]) if table.has_key("lastAlterTime") else None)
          schema_file_writer.append(dataset_scehma_record)

          dataset_idx += 1
          self.dataset_dict[dataset_urn] = dataset_idx

          for fields in field_detail_list:
            field_record = DatasetFieldRecord(fields)
            field_file_writer.append(field_record)



      instance_file_writer.flush()
      schema_file_writer.flush()
      field_file_writer.flush()
      self.logger.info("%20s contains %6d tables" % (one_db_info['database'], i))

    instance_file_writer.close()
    schema_file_writer.close()
    field_file_writer.close()

  def convert_timestamp(self, time_string):
    return int(time.mktime(time.strptime(time_string, "%Y-%m-%d %H:%M:%S")))


if __name__ == "__main__":
  args = sys.argv[1]
  t = HiveTransform()
  try:
    t.transform(args[Constant.HIVE_SCHEMA_JSON_FILE_KEY],
                args[Constant.HIVE_INSTANCE_CSV_FILE_KEY],
                args[Constant.HIVE_SCHEMA_CSV_FILE_KEY],
                args[Constant.HIVE_FIELD_METADATA_KEY])
  finally:
    t.curs.close()
    t.conn_hms.close()
    t.instance_writer.close()



