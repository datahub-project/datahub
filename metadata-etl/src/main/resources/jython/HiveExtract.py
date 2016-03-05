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

from org.slf4j import LoggerFactory
from com.ziclix.python.sql import zxJDBC
import sys, os, re, json
import datetime
from wherehows.common import Constant


class TableInfo:
  """ Class to define the variable name  """
  table_name = 'name'
  type = 'type'
  serialization_format = 'serialization_format'
  create_time = 'create_time'
  schema_url = 'schema_url'
  field_delimiter = 'field_delimiter'
  db_id = 'DB_ID'
  table_id = 'TBL_ID'
  serde_id = 'SD_ID'
  table_type = 'tbl_type'
  location = 'location'
  view_expended_text = 'view_expanded_text'
  input_format = 'input_format'
  output_format = 'output_format'
  is_compressed = 'is_compressed'
  is_storedassubdirectories = 'is_storedassubdirectories'
  etl_source = 'etl_source'

  field_list = 'field_list'
  schema_literal = 'schema_literal'

  optional_prop = [create_time, serialization_format, field_delimiter, schema_url, db_id, table_id, serde_id,
                   table_type, location, view_expended_text, input_format, output_format, is_compressed,
                   is_storedassubdirectories, etl_source]


class HiveExtract:
  """
  Extract hive metadata from hive metastore. store it in a json file
  """
  conn_hms = None
  db_dict = {}  # name : index
  table_dict = {}  # fullname : index
  serde_param_columns = []

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def get_table_info_from_v2(self, database_name):
    """
    get table, column info from table columns_v2
    :param database_name:
    :return: (DB_NAME, TBL_NAME, SERDE_FORMAT, TBL_CREATE_TIME, INTEGER_IDX, COLUMN_NAME, TYPE_NAME, COMMENT
    DB_ID, TBL_ID, SD_ID, LOCATION, VIEW_EXPANDED_TEXT, TBL_TYPE, INPUT_FORMAT)
    """
    curs = self.conn_hms.cursor()
    tbl_info_sql = """select d.NAME DB_NAME, t.TBL_NAME TBL_NAME,
      case when s.INPUT_FORMAT like '%.TextInput%' then 'Text'
        when s.INPUT_FORMAT like '%.Avro%' then 'Avro'
        when s.INPUT_FORMAT like '%.RCFile%' then 'RC'
        when s.INPUT_FORMAT like '%.Orc%' then 'ORC'
        when s.INPUT_FORMAT like '%.Sequence%' then 'Sequence'
        when s.INPUT_FORMAT like '%.Parquet%' then 'Parquet'
        else s.INPUT_FORMAT
      end SerializationFormat,
      t.CREATE_TIME TableCreateTime,
      t.DB_ID, t.TBL_ID, s.SD_ID,
      substr(s.LOCATION, length(substring_index(s.LOCATION, '/', 3))+1) Location,
      t.TBL_TYPE, t.VIEW_EXPANDED_TEXT, s.INPUT_FORMAT, s.OUTPUT_FORMAT, s.IS_COMPRESSED, s.IS_STOREDASSUBDIRECTORIES,
      c.INTEGER_IDX, c.COLUMN_NAME, c.TYPE_NAME, c.COMMENT
    from TBLS t join DBS d on t.DB_ID=d.DB_ID
    join SDS s on t.SD_ID = s.SD_ID
    join COLUMNS_V2 c on s.CD_ID = c.CD_ID
    where
    d.NAME in ('{db_name}')
    order by 1,2
    """.format(db_name=database_name)
    curs.execute(tbl_info_sql)
    rows = curs.fetchall()
    curs.close()
    return rows

  def get_table_info_from_serde_params(self, database_name):
    """
    get table, column info {MANAGED and EXTERNAL} from avro schema parameter
    :param database_name:
    :return: (DatabaseName, TableName, SerializationFormat, Create_Time, SchemaLiteral, SchemaUrl, FieldDelimiter, DB_ID, TBL_ID, SD_ID
    TBL_TYPE, INPUT_FORMAT, OUTPUT_FORMAT, IS_COMPRESSED, IS_STOREDASSUBDIRECTORIES, LOCATION, VIEW_EXPANDED_TEXT)
    """
    curs_et = self.conn_hms.cursor()
    tbl_info_sql = """select d.NAME DatabaseName, et.TBL_NAME TableName,
       case when s.INPUT_FORMAT like '%.TextInput%' then 'Text'
            when s.INPUT_FORMAT like '%.Avro%' then 'Avro'
            when s.INPUT_FORMAT like '%.RCFile%' then 'RC'
            when s.INPUT_FORMAT like '%.Orc%' then 'ORC'
            when s.INPUT_FORMAT like '%.Sequence%' then 'Sequence'
            when s.INPUT_FORMAT like '%.Parquet%' then 'Parquet'
            else s.INPUT_FORMAT
       end SerializationFormat,
       et.CREATE_TIME TableCreateTime, et.DB_ID, et.TBL_ID, s.SD_ID,
       substr(s.LOCATION, length(substring_index(s.LOCATION, '/', 3))+1) Location,
       et.TBL_TYPE, et.VIEW_EXPANDED_TEXT, s.INPUT_FORMAT, s.OUTPUT_FORMAT, s.IS_COMPRESSED, s.IS_STOREDASSUBDIRECTORIES,
       et.schema_literal SchemaLiteral, et.schema_url SchemaUrl, et.field_delim FieldDelimiter
      from (
      select t.DB_ID, t.TBL_ID, sp.SERDE_ID,
             t.TBL_NAME, t.CREATE_TIME, t.TBL_TYPE, t.VIEW_EXPANDED_TEXT,
        replace(max( case when param_key in ('avro.schema.literal', 'schema.literal')
                  then param_value
             end), '\\n', ' ') schema_literal,
        max( case when param_key in ('avro.schema.url', 'schema.url')
                  then param_value
             end) schema_url,
        max( case when param_key in ('field.delim')
                  then param_value
             end) field_delim
      from SERDE_PARAMS sp join TBLS t on sp.SERDE_ID = t.SD_ID
      where sp.PARAM_KEY regexp 'schema.literal|schema.url|field.delim'
        and sp.PARAM_VALUE regexp """ + r" '^(,|{|\\\\|\\|)' " + """
      group by 1,2,3,4,5 ) et
      JOIN DBS d on et.DB_ID = d.DB_ID
      JOIN SDS s on et.SERDE_ID = s.SD_ID
      where d.NAME = '{db_name}'
      order by 1,2 """.format(db_name=database_name)
    curs_et.execute(tbl_info_sql)
    rows = curs_et.fetchall()
    curs_et.close()
    return rows

  def format_table_metadata_v2(self, rows, schema):
    """
    process info get from COLUMN_V2 into final table, several lines form one table info
    :param rows: the info get from COLUMN_V2 table, order by table name, database name
    :param schema: {database : _, type : _, tables : [{}, {} ...] }
    :return:
    """
    db_idx = len(schema) - 1
    table_idx = -1

    field_list = []
    for row_index, row_value in enumerate(rows):
      print row_index, row_value
      print field_list

      field_list.append({'IntegerIndex': row_value[14], 'ColumnName': row_value[15], 'TypeName': row_value[16], # TODO the type name need to process
                         'Comment': row_value[17]})
      if row_index == len(rows) - 1 or (row_value[0] != rows[row_index+1][0] or row_value[1] != rows[row_index+1][1]): # if this is last record of current table
        # process the record of table
        table_record = {TableInfo.table_name: row_value[1], TableInfo.type: 'Table', TableInfo.serialization_format: row_value[2],
                      TableInfo.create_time: row_value[3], TableInfo.db_id: row_value[4], TableInfo.table_id: row_value[5],
                      TableInfo.serde_id: row_value[6], TableInfo.location: row_value[7], TableInfo.table_type: row_value[8],
                      TableInfo.view_expended_text: row_value[9], TableInfo.input_format: row_value[10], TableInfo.output_format: row_value[11],
                      TableInfo.is_compressed: row_value[12], TableInfo.is_storedassubdirectories: row_value[13],
                      TableInfo.etl_source: 'COLUMN_V2', TableInfo.field_list: field_list[:]}
        field_list = [] # empty it

        if row_value[0] not in self.db_dict:
          schema.append({'database': row_value[0], 'type': 'Hive', 'tables': []})
          db_idx += 1
          self.db_dict[row_value[0]] = db_idx

        full_name = row_value[0] + '.' + row_value[1]

        # put in schema result
        if full_name not in self.table_dict:
          schema[db_idx]['tables'].append(table_record)
          table_idx += 1
          self.table_dict[full_name] = table_idx


    self.logger.info("%s %6d tables processed for database %12s from COLUMN_V2" % (
      datetime.datetime.now(), table_idx + 1, row_value[0]))

  def format_table_metadata_serde(self, rows, schema):
    """
    add table info from rows into schema
    also add extra info.
    :param rows:  DatabaseName, TableName, SerializationFormat, Create_Time, SchemaLiteral, SchemaUrl, FieldDelimiter, DB_ID, TBL_ID, SD_ID
    :param schema: {database : _, type : _, tables : ['name' : _, ... '' : _] }
    :return:
    """

    db_idx = len(schema) - 1
    table_idx = -1
    for row_value in rows:
      if row_value[0] not in self.db_dict:
        schema.append({'database': row_value[0], 'type': 'Hive', 'tables': []})
        db_idx += 1
        self.db_dict[row_value[0]] = db_idx
      else:
        db_idx = self.db_dict[row_value[0]]
      full_name = ''
      if row_value[0]:
        full_name = row_value[0]
        if row_value[1]:
          full_name += '.' + row_value[1]
      elif row_value[1]:
        full_name = row_value[1]

      # put in schema result
      if full_name not in self.table_dict:
        schema[db_idx]['tables'].append(
          {TableInfo.table_name: row_value[1], TableInfo.type: 'Table', TableInfo.serialization_format: row_value[2],
           TableInfo.create_time: row_value[3], TableInfo.db_id: row_value[4], TableInfo.table_id: row_value[5],
           TableInfo.serde_id: row_value[6], TableInfo.location: row_value[7], TableInfo.table_type: row_value[8],
           TableInfo.view_expended_text: row_value[9], TableInfo.input_format: row_value[10],
           TableInfo.output_format: row_value[11], TableInfo.is_compressed: row_value[12],
           TableInfo.is_storedassubdirectories: row_value[13], TableInfo.etl_source: 'SERDE_PARAMS',
           TableInfo.schema_literal: row_value[14], TableInfo.schema_url: row_value[15],
           TableInfo.field_delimiter: row_value[16]})
        table_idx += 1
        self.table_dict[full_name] = table_idx

    self.logger.info("%s %6d tables processed for database %12s from SERDE_PARAM" % (
      datetime.datetime.now(), table_idx + 1, row_value[0]))

  def run(self, schema_output_file, sample_output_file):
    """
    The entrance of the class, extract schema.
    One database per time
    :param schema_output_file: output file
    :return:
    """
    cur = self.conn_hms.cursor()
    schema = []

    schema_json_file = open(schema_output_file, 'wb')
    os.chmod(schema_output_file, 0666)

    # open(sample_output_file, 'wb')
    # os.chmod(sample_output_file, 0666)
    # sample_file_writer = FileWriter(sample_output_file)

    for database_name in self.databases:
      self.logger.info("Collecting hive tables in database : " + database_name)
      # tables from schemaLiteral
      rows = []
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      rows.extend(self.get_table_info_from_serde_params(database_name))
      if len(rows) > 0:
        self.format_table_metadata_serde(rows, schema)
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Get table info from Serde %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

      # tables from Column V2
      rows = []
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      rows.extend(self.get_table_info_from_v2(database_name))
      if len(rows) > 0:
        self.format_table_metadata_v2(rows, schema)
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Get table info from COLUMN_V2 %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

    schema_json_file.write(json.dumps(schema, indent=None) + '\n')

    cur.close()
    schema_json_file.close()

  def get_all_databases(self, database_white_list):
    """
    Fetch all databases name from DBS table
    :return:
    """
    database_white_list = ",".join(["'" + x + "'" for x in database_white_list.split(',')])
    fetch_all_database_names = "SELECT `NAME` FROM DBS WHERE NAME NOT LIKE 'u\\_%' OR NAME IN ({white_list})"\
      .format(white_list=database_white_list)
    self.logger.info(fetch_all_database_names)
    curs = self.conn_hms.cursor()
    curs.execute(fetch_all_database_names)
    rows = [item[0] for item in curs.fetchall()]
    curs.close()
    return rows


if __name__ == "__main__":
  args = sys.argv[1]

  # connection
  username = args[Constant.HIVE_METASTORE_USERNAME]
  password = args[Constant.HIVE_METASTORE_PASSWORD]
  jdbc_driver = args[Constant.HIVE_METASTORE_JDBC_DRIVER]
  jdbc_url = args[Constant.HIVE_METASTORE_JDBC_URL]

  if Constant.HIVE_DATABASE_WHITELIST_KEY in args:
    database_white_list = args[Constant.HIVE_DATABASE_WHITELIST_KEY]
  else:
    database_white_list = ''

  e = HiveExtract()
  e.conn_hms = zxJDBC.connect(jdbc_url, username, password, jdbc_driver)

  try:
    e.databases = e.get_all_databases(database_white_list)
    e.run(args[Constant.HIVE_SCHEMA_JSON_FILE_KEY], None)
  finally:
    e.conn_hms.close()
