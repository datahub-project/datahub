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
import sys, os, re, json, csv
import datetime
import SchemaUrlHelper
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import HiveDependencyInstanceRecord
from wherehows.common import Constant


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

  field_list = 'fields'
  schema_literal = 'schema_literal'

  optional_prop = [create_time, serialization_format, field_delimiter, schema_url, db_id, table_id, serde_id,
                   table_type, location, view_expended_text, input_format, output_format, is_compressed,
                   is_storedassubdirectories, etl_source]

class HiveExtract:
  """
  Extract hive metadata from hive metastore in mysql. store it in a json file
  """
  conn_hms = None
  db_dict = {}  # name : index
  table_dict = {}  # fullname : index
  dataset_dict = {}  # name : index
  instance_dict = {}  # name : index
  serde_param_columns = []

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def get_table_info_from_v2(self, database_name, is_dali=False):
    """
    get table, column info from table columns_v2
    :param database_name:
    :return: (0 DB_NAME, 1 TBL_NAME, 2 SERDE_FORMAT, 3 TBL_CREATE_TIME
    4 DB_ID, 5 TBL_ID,6 SD_ID, 7 LOCATION, 8 VIEW_EXPANDED_TEXT, 9 TBL_TYPE, 10 VIEW_EXPENDED_TEXT, 11 INPUT_FORMAT,12  OUTPUT_FORMAT,
    13IS_COMPRESSED, 14 IS_STOREDASSUBDIRECTORIES, 15 INTEGER_IDX, 16 COLUMN_NAME, 17 TYPE_NAME, 18 COMMENT)
    """
    curs = self.conn_hms.cursor()
    if is_dali:
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
        end logical_name, unix_timestamp(now()) created_time, concat('dalids:///', d.NAME, '/', t.TBL_NAME) dataset_urn
      from TBLS t join DBS d on t.DB_ID=d.DB_ID
      join SDS s on t.SD_ID = s.SD_ID
      join COLUMNS_V2 c on s.CD_ID = c.CD_ID
      where
      d.NAME in ('{db_name}') and (d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and d.NAME not like 'dalitest%' and t.TBL_TYPE = 'VIRTUAL_VIEW'
      order by DB_NAME, dataset_name, version DESC
      """.format(version='{version}', db_name=database_name)
    else:
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
        c.INTEGER_IDX, c.COLUMN_NAME, c.TYPE_NAME, c.COMMENT, t.TBL_NAME dataset_name, 0 version, 'Hive' TYPE,
        case when LOCATE('view', LOWER(t.TBL_TYPE)) > 0 then 'View'
          when LOCATE('index', LOWER(t.TBL_TYPE)) > 0 then 'Index'
          else 'Table'
        end storage_type, concat(d.NAME, '.', t.TBL_NAME) native_name, t.TBL_NAME logical_name,
        unix_timestamp(now()) created_time, concat('hive:///', d.NAME, '/', t.TBL_NAME) dataset_urn
      from TBLS t join DBS d on t.DB_ID=d.DB_ID
      join SDS s on t.SD_ID = s.SD_ID
      join COLUMNS_V2 c on s.CD_ID = c.CD_ID
      where
      d.NAME in ('{db_name}') and not ((d.NAME like '%\_mp' or d.NAME like '%\_mp\_versioned') and t.TBL_TYPE = 'VIRTUAL_VIEW')
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

        table_record = {TableInfo.table_name: row_value[1], TableInfo.type: row_value[21],
                      TableInfo.native_name: row_value[22], TableInfo.logical_name: row_value[23],
                      TableInfo.dataset_name: row_value[18], TableInfo.version: str(row_value[19]),
                      TableInfo.serialization_format: row_value[2],
                      TableInfo.create_time: row_value[3], TableInfo.db_id: row_value[4], TableInfo.table_id: row_value[5],
                      TableInfo.serde_id: row_value[6], TableInfo.location: row_value[7], TableInfo.table_type: row_value[8],
                      TableInfo.view_expended_text: row_value[9], TableInfo.input_format: row_value[10], TableInfo.output_format: row_value[11],
                      TableInfo.is_compressed: row_value[12], TableInfo.is_storedassubdirectories: row_value[13],
                      TableInfo.etl_source: 'COLUMN_V2', TableInfo.field_list: field_list[:]}

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

      literal = None
      if row_value[15] and not row_value[14]:  # schema_url is available but missing schema_literal
        try:
          literal = self.get_schema_literal_from_url(row_value[15])
        except Exception as e:
          self.logger.error(str(e))
      elif row_value[14]:
        literal = row_value[14]

      # put in schema result
      if full_name not in self.table_dict:
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

    self.logger.info("%s %6d tables processed for database %12s from SERDE_PARAM" % (
      datetime.datetime.now(), table_idx + 1, row_value[0]))

  def run(self, schema_output_file, sample_output_file=None, hdfs_map_output_file=None,
          hdfs_namenode_ipc_uri=None, kerberos_auth=False, kerberos_principal=None, keytab_file=None):
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

    if type(kerberos_auth) == str:
      if kerberos_auth.lower() == 'false':
        kerberos_auth = False
      else:
        kerberos_auth = True 
    self.schema_url_helper = SchemaUrlHelper.SchemaUrlHelper(hdfs_namenode_ipc_uri, kerberos_auth, kerberos_principal, keytab_file)

    for database_name in self.databases:
      self.logger.info("Collecting hive tables in database : " + database_name)

      # tables from Column V2
      rows = []
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      rows.extend(self.get_table_info_from_v2(database_name, False))
      if len(rows) > 0:
        self.format_table_metadata_v2(rows, schema)
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Get Hive table info from COLUMN_V2 %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

      rows = []
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      rows.extend(self.get_table_info_from_v2(database_name, True))
      if len(rows) > 0:
        self.format_table_metadata_v2(rows, schema)
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Get Dalids table info from COLUMN_V2 %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

      # tables from schemaLiteral
      rows = []
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      rows.extend(self.get_table_info_from_serde_params(database_name))
      if len(rows) > 0:
        self.format_table_metadata_serde(rows, schema)
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Get table info from Serde %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

    schema_json_file.write(json.dumps(schema, indent=None) + '\n')

    schema_json_file.close()

    # fetch hive (managed/external) table to hdfs path mapping
    rows = []
    hdfs_map_csv_file = open(hdfs_map_output_file, 'wb')
    os.chmod(hdfs_map_output_file, 0666)
    begin = datetime.datetime.now().strftime("%H:%M:%S")
    rows = self.get_hdfs_map()
    hdfs_map_columns = ['db_name', 'table_name', 'cluster_uri', 'abstract_hdfs_path']
    csv_writer = csv.writer(hdfs_map_csv_file, delimiter='\x1a', lineterminator='\n', quoting=csv.QUOTE_NONE)
    csv_writer.writerow(hdfs_map_columns)
    csv_writer.writerows(rows)
    end = datetime.datetime.now().strftime("%H:%M:%S")
    self.logger.info("Get hdfs map from SDS %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

    cur.close()
    hdfs_map_csv_file.close()

  def get_all_databases(self, database_white_list, database_black_list):
    """
    Fetch all databases name from DBS table
    :return:
    """
    database_white_list = ",".join(["'" + x + "'" for x in database_white_list.split(',')])
    database_black_list = ",".join(["'" + x + "'" for x in database_black_list.split(',')])
    fetch_all_database_names = "SELECT `NAME` FROM DBS WHERE `NAME` IN ({white_list}) OR NOT (`NAME` IN ({black_list}) OR `NAME` LIKE 'u\\_%')"\
      .format(white_list=database_white_list, black_list=database_black_list)
    self.logger.info(fetch_all_database_names)
    curs = self.conn_hms.cursor()
    curs.execute(fetch_all_database_names)
    rows = [item[0] for item in curs.fetchall()]
    curs.close()
    return rows

  def get_schema_literal_from_url(self, schema_url):
    """
    fetch avro schema literal from
    - avsc file on hdfs via hdfs/webhdfs
    - json string in schemaregistry via http
    :param schema_url:  e.g. hdfs://server:port/data/tracking/abc/_schema.avsc http://schema-registry-vip-1:port/schemaRegistry/schemas/latest_with_type=xyz
    :param schema: {database : _, type : _, tables : ['name' : _, ... '' : _] }
    :return: json string of avro schema
    """
    if schema_url.startswith('hdfs://') or schema_url.startswith('/') or schema_url.startswith('webhdfs://'):
      return self.schema_url_helper.get_from_hdfs(schema_url)
    elif scheam_url.startswith('https://') or schema_url.startswith('http://'):
      return self.schema_url_helper.get_from_http(schema_url)
    else:
      self.logger.error("get_schema_literal_from_url() gets a bad input: %s" % schema_url)
      return ''

  def get_hdfs_map(self):
    """
    Fetch the mapping from hdfs location to hive (managed and external) table
    :return:
    """

    hdfs_map_sql = """select db_name, tbl_name table_name, cluster_uri,
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
from (
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

    curs = self.conn_hms.cursor()
    curs.execute(hdfs_map_sql)
    rows = curs.fetchall()
    curs.close()
    self.logger.info("%6d Hive table => HDFS path mapping relations found." % len(rows))
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
    database_white_list = "''"

  if Constant.HIVE_DATABASE_BLACKLIST_KEY in args:
    database_black_list = args[Constant.HIVE_DATABASE_BLACKLIST_KEY]
  else:
    database_black_list = "''"

  e = HiveExtract()
  e.conn_hms = zxJDBC.connect(jdbc_url, username, password, jdbc_driver)

  try:
    e.databases = e.get_all_databases(database_white_list, database_black_list)
    e.run(args[Constant.HIVE_SCHEMA_JSON_FILE_KEY], \
          None, \
          args[Constant.HIVE_HDFS_MAP_CSV_FILE_KEY], \
          args[Constant.HDFS_NAMENODE_IPC_URI_KEY], \
          args[Constant.KERBEROS_AUTH_KEY], \
          args[Constant.KERBEROS_PRINCIPAL_KEY], \
          args[Constant.KERBEROS_KEYTAB_FILE_KEY]
          )
  finally:
    e.conn_hms.close()
