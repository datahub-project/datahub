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

import csv
import json
import os
import stat
import sys
from com.ziclix.python.sql import zxJDBC
from datetime import datetime
from org.slf4j import LoggerFactory
from wherehows.common import Constant

import FileUtil
import SchemaUrlHelper


class TableInfo:
  """ Class to define the variable name  """
  table_name = 'name'
  dataset_name = 'dataset_name'
  native_name = 'native_name'
  logical_name = 'logical_name'
  version = 'version'
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
  source_modified_time = 'source_modified_time'

  field_list = 'fields'
  schema_literal = 'schema_literal'

  optional_prop = [create_time, serialization_format, field_delimiter, schema_url, db_id, table_id, serde_id,
                   table_type, location, view_expended_text, input_format, output_format, is_compressed,
                   is_storedassubdirectories, etl_source]


class HiveExtract:
  """
  Extract hive metadata from hive metastore in mysql. store it in json files
  """
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    # connection
    self.username = args[Constant.HIVE_METASTORE_USERNAME]
    self.password = args[Constant.HIVE_METASTORE_PASSWORD]
    self.jdbc_driver = args[Constant.HIVE_METASTORE_JDBC_DRIVER]
    self.jdbc_url = args[Constant.HIVE_METASTORE_JDBC_URL]
    self.connection_interval = int(args[Constant.HIVE_METASTORE_RECONNECT_TIME])
    self.logger.info("DB re-connection interval: %d" % self.connection_interval)

    self.db_whitelist = args[Constant.HIVE_DATABASE_WHITELIST_KEY] if Constant.HIVE_DATABASE_WHITELIST_KEY in args else "''"
    self.db_blacklist = args[Constant.HIVE_DATABASE_BLACKLIST_KEY] if Constant.HIVE_DATABASE_BLACKLIST_KEY in args else "''"
    self.logger.info("DB whitelist: " + self.db_whitelist)
    self.logger.info("DB blacklist: " + self.db_blacklist)

    self.conn_hms = None
    self.connect_time = None
    self.db_connect(True)

    hdfs_namenode_ipc_uri = args.get(Constant.HDFS_NAMENODE_IPC_URI_KEY, None)
    kerberos_principal = args.get(Constant.KERBEROS_PRINCIPAL_KEY, None)
    keytab_file = args.get(Constant.KERBEROS_KEYTAB_FILE_KEY, None)

    kerberos_auth = False
    if Constant.KERBEROS_AUTH_KEY in args:
      kerberos_auth = FileUtil.parse_bool(args[Constant.KERBEROS_AUTH_KEY], False)

    self.schema_url_helper = SchemaUrlHelper.SchemaUrlHelper(hdfs_namenode_ipc_uri, kerberos_auth, kerberos_principal, keytab_file)

    # global variables
    self.databases = None
    self.db_dict = {}  # name : index
    self.table_dict = {}  # fullname : index
    self.dataset_dict = {}  # name : index
    self.instance_dict = {}  # name : index
    self.serde_param_columns = []
    # counting statistics
    self.external_url = 0
    self.hdfs_count = 0
    self.schema_registry_count = 0


  def get_table_info_from_v2(self, database_name, is_dali=False):
    """
    get table, column info from table columns_v2
    :param database_name:
    :param is_dali: default False
    :return: (0 DB_NAME, 1 TBL_NAME, 2 SERDE_FORMAT, 3 TBL_CREATE_TIME
    4 DB_ID, 5 TBL_ID,6 SD_ID, 7 LOCATION, 8 TBL_TYPE, 9 VIEW_EXPENDED_TEXT, 10 INPUT_FORMAT,11  OUTPUT_FORMAT,
    12 IS_COMPRESSED, 13 IS_STOREDASSUBDIRECTORIES, 14 INTEGER_IDX, 15 COLUMN_NAME, 16 TYPE_NAME, 17 COMMENT
    18 dataset_name, 19 version, 20 TYPE, 21 storage_type, 22 native_name, 23 logical_name, 24 created_time,
    25 dataset_urn, 26 source_modified_time)
    """
    if is_dali:
      tbl_info_sql = """SELECT d.NAME DB_NAME, t.TBL_NAME TBL_NAME,
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
        c.INTEGER_IDX, c.COLUMN_NAME, c.TYPE_NAME, c.COMMENT,
        case when t.TBL_NAME regexp '_[0-9]+_[0-9]+_[0-9]+$'
          then concat(substring(t.TBL_NAME, 1, length(t.TBL_NAME) - length(substring_index(t.TBL_NAME, '_', -3)) - 1),'_{version}')
        else t.TBL_NAME
        end dataset_name,
        case when t.TBL_NAME regexp '_[0-9]+_[0-9]+_[0-9]+$'
          then replace(substring_index(t.TBL_NAME, '_', -3), '_', '.')
          else 0
        end version, 'Dalids' TYPE, 'View' storage_type, concat(d.NAME, '.', t.TBL_NAME) native_name,
        case when t.TBL_NAME regexp '_[0-9]+_[0-9]+_[0-9]+$'
          then substring(t.TBL_NAME, 1, length(t.TBL_NAME) - length(substring_index(t.TBL_NAME, '_', -3)) - 1)
          else t.TBL_NAME
        end logical_name, unix_timestamp(now()) created_time, concat('dalids:///', d.NAME, '/', t.TBL_NAME) dataset_urn,
        p.PARAM_VALUE source_modified_time
      FROM TBLS t join DBS d on t.DB_ID=d.DB_ID
      JOIN SDS s on t.SD_ID = s.SD_ID
      JOIN COLUMNS_V2 c on s.CD_ID = c.CD_ID
      JOIN TABLE_PARAMS p on p.TBL_ID = t.TBL_ID
      WHERE p.PARAM_KEY = 'transient_lastDdlTime' and
        d.NAME in ('{db_name}') and (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and d.NAME not like 'dalitest%' and t.TBL_TYPE = 'VIRTUAL_VIEW'
      order by DB_NAME, dataset_name, version DESC
      """.format(version='{version}', db_name=database_name)
    else:
      tbl_info_sql = """SELECT d.NAME DB_NAME, t.TBL_NAME TBL_NAME,
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
        c.INTEGER_IDX, c.COLUMN_NAME, c.TYPE_NAME, c.COMMENT, t.TBL_NAME dataset_name, 0 version, 'Hive' TYPE,
        case when LOCATE('view', LOWER(t.TBL_TYPE)) > 0 then 'View'
          when LOCATE('index', LOWER(t.TBL_TYPE)) > 0 then 'Index'
          else 'Table'
        end storage_type, concat(d.NAME, '.', t.TBL_NAME) native_name, t.TBL_NAME logical_name,
        unix_timestamp(now()) created_time, concat('hive:///', d.NAME, '/', t.TBL_NAME) dataset_urn,
        p.PARAM_VALUE source_modified_time
      FROM TBLS t join DBS d on t.DB_ID=d.DB_ID
      JOIN SDS s on t.SD_ID = s.SD_ID
      JOIN COLUMNS_V2 c on s.CD_ID = c.CD_ID
      JOIN TABLE_PARAMS p on p.TBL_ID = t.TBL_ID
      WHERE p.PARAM_KEY = 'transient_lastDdlTime' and
        d.NAME in ('{db_name}') and not ((d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and t.TBL_TYPE = 'VIRTUAL_VIEW')
      order by 1,2
      """.format(db_name=database_name)
    return self.execute_fetchall(tbl_info_sql)


  def get_table_info_from_serde_params(self, database_name):
    """
    get table, column info {MANAGED and EXTERNAL} from avro schema parameter
    :param database_name:
    :return: (DatabaseName, TableName, SerializationFormat, Create_Time, SchemaLiteral, SchemaUrl, FieldDelimiter, DB_ID, TBL_ID, SD_ID
    TBL_TYPE, INPUT_FORMAT, OUTPUT_FORMAT, IS_COMPRESSED, IS_STOREDASSUBDIRECTORIES, LOCATION, VIEW_EXPANDED_TEXT)
    """
    tbl_info_sql = """SELECT d.NAME DatabaseName, t.TBL_NAME TableName,
       case when s.INPUT_FORMAT like '%.TextInput%' then 'Text'
            when s.INPUT_FORMAT like '%.Avro%' then 'Avro'
            when s.INPUT_FORMAT like '%.RCFile%' then 'RC'
            when s.INPUT_FORMAT like '%.Orc%' then 'ORC'
            when s.INPUT_FORMAT like '%.Sequence%' then 'Sequence'
            when s.INPUT_FORMAT like '%.Parquet%' then 'Parquet'
            else s.INPUT_FORMAT
       end SerializationFormat,
       t.CREATE_TIME TableCreateTime, t.DB_ID, t.TBL_ID, s.SD_ID,
       substr(s.LOCATION, length(substring_index(s.LOCATION, '/', 3)) + 1) Location,
       t.TBL_TYPE, t.VIEW_EXPANDED_TEXT, s.INPUT_FORMAT, s.OUTPUT_FORMAT, s.IS_COMPRESSED, s.IS_STOREDASSUBDIRECTORIES,
       replace(max(case when param_key in ('avro.schema.literal', 'schema.literal') then param_value end), '\\n', ' ') schema_literal,
       max(case when param_key in ('avro.schema.url', 'schema.url') then param_value end) schema_url,
       max( case when param_key in ('field.delim') then param_value end) field_delim
      FROM TBLS t
      JOIN SERDE_PARAMS sp on sp.SERDE_ID = t.SD_ID
      JOIN DBS d on t.DB_ID = d.DB_ID
      JOIN SDS s on t.SD_ID = s.SD_ID
      WHERE d.NAME = '{db_name}' and 
        sp.PARAM_KEY in ('avro.schema.literal', 'schema.literal', 'avro.schema.url', 'schema.url', 'field.delim')
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
      order by 1,2 """.format(db_name=database_name)
    return self.execute_fetchall(tbl_info_sql)


  def format_table_metadata_v2(self, rows, schema):
    """
    process info get from COLUMN_V2 into final table, several lines form one table info
    :param rows: the info get from COLUMN_V2 table, order by database name, table name
    :param schema: {database : _, type : _, tables : [{}, {} ...] }
    :return:
    """
    db_idx = len(schema) - 1
    table_idx = -1

    field_list = []
    for row_index, row_value in enumerate(rows):

      field_list.append({'IntegerIndex': row_value[14], 'ColumnName': row_value[15], 'TypeName': row_value[16],
                         'Comment': row_value[17]})
      if row_index == len(rows) - 1 or (row_value[0] != rows[row_index+1][0] or row_value[1] != rows[row_index+1][1]): # if this is last record of current table
        # sort the field_list by IntegerIndex
        field_list = sorted(field_list, key=lambda k: k['IntegerIndex'])
        # process the record of table

        table_record = {TableInfo.table_name: row_value[1],
                        TableInfo.type: row_value[21],
                        TableInfo.native_name: row_value[22],
                        TableInfo.logical_name: row_value[23],
                        TableInfo.dataset_name: row_value[18],
                        TableInfo.version: str(row_value[19]),
                        TableInfo.serialization_format: row_value[2],
                        TableInfo.create_time: row_value[3],
                        TableInfo.source_modified_time: row_value[26],
                        TableInfo.db_id: row_value[4],
                        TableInfo.table_id: row_value[5],
                        TableInfo.serde_id: row_value[6],
                        TableInfo.location: row_value[7],
                        TableInfo.table_type: row_value[8],
                        TableInfo.view_expended_text: row_value[9],
                        TableInfo.input_format: row_value[10],
                        TableInfo.output_format: row_value[11],
                        TableInfo.is_compressed: row_value[12],
                        TableInfo.is_storedassubdirectories: row_value[13],
                        TableInfo.etl_source: 'COLUMN_V2',
                        TableInfo.field_list: field_list[:]}

        field_list = [] # empty it

        if row_value[0] not in self.db_dict:
          schema.append({'database': row_value[0], 'type': row_value[20], 'tables': []})
          db_idx += 1
          self.db_dict[row_value[0]] = db_idx

        full_name = row_value[0] + '.' + row_value[1]

        # put in schema result
        if full_name not in self.table_dict:
          schema[db_idx]['tables'].append(table_record)
          table_idx += 1
          self.table_dict[full_name] = table_idx

    #self.logger.info("%4d tables processed from COLUMN_V2" % (table_idx + 1))


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

      full_name = row_value[0] + '.' + row_value[1]
      if full_name in self.table_dict:
        continue

      literal = None
      if row_value[15] and not row_value[14]:  # schema_url is available but missing schema_literal
        try:
          self.external_url += 1
          literal = self.get_schema_literal_from_url(row_value[15])
        except Exception as e:
          self.logger.error(str(e))
      elif row_value[14]:
        literal = row_value[14]

      # put in schema result
      schema[db_idx]['tables'].append(
        {TableInfo.table_name: row_value[1], TableInfo.type: 'Table', TableInfo.serialization_format: row_value[2],
         TableInfo.dataset_name: row_value[1],
         TableInfo.native_name: row_value[1], TableInfo.logical_name: row_value[1], TableInfo.version: '0',
         TableInfo.create_time: row_value[3], TableInfo.db_id: row_value[4], TableInfo.table_id: row_value[5],
         TableInfo.serde_id: row_value[6], TableInfo.location: row_value[7], TableInfo.table_type: row_value[8],
         TableInfo.view_expended_text: row_value[9], TableInfo.input_format: row_value[10],
         TableInfo.output_format: row_value[11], TableInfo.is_compressed: row_value[12],
         TableInfo.is_storedassubdirectories: row_value[13], TableInfo.etl_source: 'SERDE_PARAMS',
         TableInfo.schema_literal: literal,
         TableInfo.schema_url: row_value[15],
         TableInfo.field_delimiter: row_value[16]})
      table_idx += 1
      self.table_dict[full_name] = table_idx

    self.logger.info("%4d tables processed from SERDE_PARAM" % (table_idx + 1))


  def run(self, schema_output_file, sample_output_file=None, hdfs_map_output_file=None):
    """
    The entrance of the class, extract schema.
    One database per time
    :param schema_output_file: schema output file
    :param sample_output_file: sample output file
    :param hdfs_map_output_file: hdfs map output file
    :return:
    """
    self.logger.info("Total %d databases" % len(self.databases))
    schema = []

    # open(sample_output_file, 'wb')
    # os.chmod(sample_output_file, stat.S_IWOTH)
    # sample_file_writer = FileWriter(sample_output_file)

    db_count = 0
    hive_count = 0
    dali_count = 0
    serde_count = 0
    for database_name in self.databases:
      db_count += 1
      self.db_connect()
      self.logger.info("#%d Collecting tables in database: %s" % (db_count, database_name))

      # tables from Column V2
      rows = self.get_table_info_from_v2(database_name, False)
      hive_count += len(rows)
      self.logger.info(" Get %d Hive table info from COLUMN_V2" % len(rows))
      if len(rows) > 0:
        self.format_table_metadata_v2(rows, schema)

      rows = self.get_table_info_from_v2(database_name, True)
      self.logger.info(" Get %d Dalids table info from COLUMN_V2" % len(rows))
      dali_count += len(rows)
      if len(rows) > 0:
        self.format_table_metadata_v2(rows, schema)

      # tables from schemaLiteral
      rows = self.get_table_info_from_serde_params(database_name)
      serde_count += len(rows)
      self.logger.info(" Get %d table info from Serde" % len(rows))
      if len(rows) > 0:
        self.format_table_metadata_serde(rows, schema)

    self.logger.info(
      "Hive tables: %d, Dalids tables: %d, SerDe tables: %d" % (hive_count, dali_count, serde_count))
    self.logger.info("External URL visited: %d, hdfs %d, schema registry %d" % (
      self.external_url, self.hdfs_count, self.schema_registry_count))

    with open(schema_output_file, 'wb') as schema_json_file:
      os.chmod(schema_output_file,
               stat.S_IWUSR | stat.S_IRUSR | stat.S_IWGRP | stat.S_IRGRP | stat.S_IWOTH | stat.S_IROTH)
      for item in schema:
        schema_json_file.write("{}\n".format(json.dumps(item, separators=(',',':'))))

    # fetch hive (managed/external) table to hdfs path mapping
    begin = datetime.now().strftime("%H:%M:%S")
    rows = self.get_hdfs_map()
    self.logger.info("%4d Hive table => HDFS path mapping relations found." % len(rows))

    with open(hdfs_map_output_file, 'wb') as hdfs_map_csv_file:
      os.chmod(hdfs_map_output_file,
               stat.S_IWUSR | stat.S_IRUSR | stat.S_IWGRP | stat.S_IRGRP | stat.S_IWOTH | stat.S_IROTH)
      hdfs_map_columns = ['db_name', 'table_name', 'cluster_uri', 'abstract_hdfs_path']
      csv_writer = csv.writer(hdfs_map_csv_file, delimiter='\x1a', lineterminator='\n', quoting=csv.QUOTE_NONE)
      csv_writer.writerow(hdfs_map_columns)
      for row in rows:
        try:
          csv_writer.writerow(row)
        except:
          self.logger.error("Error writing CSV row: " + str(row))
    end = datetime.now().strftime("%H:%M:%S")
    self.logger.info("Get hdfs map from SDS [%s -> %s]\n" % (str(begin), str(end)))


  def get_all_databases(self, database_white_list, database_black_list):
    """
    Fetch all databases name from DBS table
    :return:
    """
    white_list = ",".join(["'" + x + "'" for x in database_white_list.split(',')])
    black_list = ",".join(["'" + x + "'" for x in database_black_list.split(',')])
    fetch_all_database_names = "SELECT `NAME` FROM DBS WHERE `NAME` IN ({white_list}) OR NOT (`NAME` IN ({black_list}) OR `NAME` LIKE 'u\\_%')"\
      .format(white_list=white_list, black_list=black_list)
    #self.logger.info(fetch_all_database_names)
    return [item[0] for item in self.execute_fetchall(fetch_all_database_names)]


  def get_schema_literal_from_url(self, schema_url):
    """
    fetch avro schema literal from
    - avsc file on hdfs via hdfs/webhdfs
    - json string in schemaregistry via http
    :param schema_url:  e.g. hdfs://server:port/data/tracking/abc/_schema.avsc http://schema-registry-vip-1:port/schemaRegistry/schemas/latest_with_type=xyz
    :return: json string of avro schema
    """
    if schema_url.startswith('hdfs://') or schema_url.startswith('/') or schema_url.startswith('webhdfs://'):
      self.hdfs_count += 1
      #return self.schema_url_helper.get_from_hdfs(schema_url)
      self.logger.info("Skip schema on HDFS: " + schema_url)
      return None
    elif schema_url.startswith('https://') or schema_url.startswith('http://'):
      self.schema_registry_count += 1
      return self.schema_url_helper.get_from_http(schema_url)
    else:
      self.logger.error("get_schema_literal_from_url() gets a bad input: %s" % schema_url)
      return None


  def get_hdfs_map(self):
    """
    Fetch the mapping from hdfs location to hive (managed and external) table
    :return:
    """
    hdfs_map_sql = """SELECT db_name, tbl_name table_name, cluster_uri,
        cast(
        case when substring_index(hdfs_path, '/', -4) regexp '[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}'
             then substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -4))-1)
             when substring_index(hdfs_path, '/', -3) regexp '[0-9]{4}/[0-9]{2}/[0-9]{2}'
             then substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -3))-1)
             when substring_index(hdfs_path, '/', -1) regexp '20[0-9]{2}([\\._-]?[0-9][0-9]){2,5}'
               or substring_index(hdfs_path, '/', -1) regexp '1[3-6][0-9]{11}(-(PT|UTC|GMT|SCN)-[0-9]+)?'
               or substring_index(hdfs_path, '/', -1) regexp '[0-9]+([\\._-][0-9]+)?'
               or substring_index(hdfs_path, '/', -1) regexp '[vV][0-9]+([\\._-][0-9]+)?'
               or substring_index(hdfs_path, '/', -1) regexp '(prod|ei|qa|dev)_[0-9]+\\.[0-9]+\\.[0-9]+_20[01][0-9]([_-]?[0-9][0-9]){2,5}'
             then substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -1))-1)
             when hdfs_path regexp '/datepartition=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
               or hdfs_path regexp '/datepartition=[[:alnum:]]+'
             then substring(hdfs_path, 1, locate('/datepartition=', hdfs_path)-1)
             when hdfs_path regexp '/date_sk=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
             then substring(hdfs_path, 1, locate('/date_sk=', hdfs_path)-1)
             when hdfs_path regexp '/ds=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
             then substring(hdfs_path, 1, locate('/ds=', hdfs_path)-1)
             when hdfs_path regexp '/dt=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
             then substring(hdfs_path, 1, locate('/dt=', hdfs_path)-1)
             when hdfs_path regexp '^/[[:alnum:]]+/[[:alnum:]]+/[[:alnum:]]+/20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring_index(hdfs_path, '/', 4), '/*',
                         substring(hdfs_path, length(substring_index(hdfs_path, '/', 5))+1))
             when hdfs_path regexp '^/[[:alnum:]]+/[[:alnum:]]+/20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring_index(hdfs_path, '/', 3), '/*',
                         substring(hdfs_path, length(substring_index(hdfs_path, '/', 4))+1))
             when substring_index(hdfs_path, '/', -3) regexp '^(prod|ei|qa|dev)_[0-9]+\\.[0-9]+\\.[0-9]+_20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -3))-1), '/*/',
                         substring_index(hdfs_path, '/', -2))
             when substring_index(hdfs_path, '/', -2) regexp '^(prod|ei|qa|dev)_[0-9]+\\.[0-9]+\\.[0-9]+_20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -2))-1), '/*/',
                         substring_index(hdfs_path, '/', -1))
             else hdfs_path
        end as char(300)) abstract_hdfs_path
      FROM (
          select d.NAME DB_NAME, t.TBL_NAME,
            substring_index(s.LOCATION, '/', 3) cluster_uri,
            substring(s.LOCATION, length(substring_index(s.LOCATION, '/', 3))+1) hdfs_path,
            length(s.LOCATION) - length(substring_index(s.LOCATION, '/', 3)) as hdfs_path_len
          from SDS s
               join TBLS t
            on s.SD_ID = t.SD_ID
               join DBS d
            on t.DB_ID = d.DB_ID
               left join PARTITIONS p
            on t.TBL_ID = p.TBL_ID
          where p.PART_ID is null
            and LOCATION is not null
          union all
          select d.NAME DB_NAME, t.TBL_NAME,
            substring_index(s.LOCATION, '/', 3) cluster_uri,
            substring(s.LOCATION, length(substring_index(s.LOCATION, '/', 3))+1) hdfs_path,
            length(s.LOCATION) - length(substring_index(s.LOCATION, '/', 3)) as hdfs_path_len
          from SDS s
               join (select TBL_ID, MAX(SD_ID) SD_ID from PARTITIONS group by 1) p
            on s.SD_ID = p.SD_ID
               join TBLS t
            on p.TBL_ID = t.TBL_ID
               join DBS d
            on t.DB_ID = d.DB_ID
          where not LOCATION like 'hdfs:%__HIVE_DEFAULT_PARTITION__%'
      ) x where hdfs_path not like '/tmp/%'
      order by 1,2"""
    return self.execute_fetchall(hdfs_map_sql)


  def execute_fetchall(self, sql):
    curs = self.conn_hms.cursor()
    curs.execute(sql)
    #self.debug(sql)
    rows = curs.fetchall()
    curs.close()
    return rows

  def db_connect(self, init=False):
    if init or (datetime.now() - self.connect_time).total_seconds() > self.connection_interval:
      if self.conn_hms:
        self.conn_hms.close()
      self.conn_hms = zxJDBC.connect(self.jdbc_url, self.username, self.password, self.jdbc_driver)
      self.logger.info("Connected to Hive metadata-store DB")
      self.connect_time = datetime.now()


if __name__ == "__main__":
  args = sys.argv[1]

  e = HiveExtract(args)

  temp_dir = FileUtil.etl_temp_dir(args, "HIVE")
  schema_json_file = os.path.join(temp_dir, args[Constant.HIVE_SCHEMA_JSON_FILE_KEY])
  hdfs_map_csv_file = os.path.join(temp_dir, args[Constant.HIVE_HDFS_MAP_CSV_FILE_KEY])

  try:
    e.databases = e.get_all_databases(e.db_whitelist, e.db_blacklist)
    e.run(schema_json_file, None, hdfs_map_csv_file)
  finally:
    if e.conn_hms:
      e.conn_hms.close()
