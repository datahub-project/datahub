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

from com.ziclix.python.sql import zxJDBC
import sys, os, re
import json, csv
import datetime
from wherehows.common.schemas import SampleDataRecord
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class OracleExtract:
  table_dict = {}
  index_dict = {}
  table_output_list = []
  field_output_list = []

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def get_view_info(self, database_name, view_name):
    '''
    :param database_name:
    :param view_name:
    :return:
    '''
    view_cols = None
    return view_cols

  def get_table_info(self, table_name, excluded_schema_list):
    '''
    get table, column info from Oracle all_tables
    :param schema_name:
    :param table_name: not used in common case
    :return:
    '''
    table_name_filter = ''
    schema_exclusion_filter = ''
    if table_name and len(table_name) > 0:
      if table_name.find('.') > 0:
        table_name_filter = " AND OWNER='%s' AND TABLE_NAME='%s' " % table_name.split('.')
      else:
        table_name_filter = " AND TABLE_NAME='%s' " % table_name
      self.logger.info("Get Oracle metadata with extra filter: %s" % table_name_filter)
    elif excluded_schema_list and len(excluded_schema_list) > 0:
      schema_exclusion_filter = " AND NOT REGEXP_LIKE(t.OWNER, '%s') " % excluded_schema_list
      self.logger.info("Get Oracle metadata with extra excluded schema: %s" % excluded_schema_list)

    curs_meta = self.conn_db.cursor()

    column_info_sql = """
    select
      t.OWNER, t.TABLE_NAME, t.PARTITIONED,
      c.COLUMN_ID, c.COLUMN_NAME, c.DATA_TYPE, c.NULLABLE,
      c.DATA_LENGTH, c.DATA_PRECISION, c.DATA_SCALE,
      c.CHAR_LENGTH, c.CHARACTER_SET_NAME,
      c.DATA_DEFAULT, m.COMMENTS
    from ALL_TABLES t
      join ALL_TAB_COLUMNS c
        on t.OWNER = c.OWNER
        and t.TABLE_NAME = c.TABLE_NAME
      left join ALL_COL_COMMENTS m
        on c.OWNER = m.OWNER
        and c.TABLE_NAME = m.TABLE_NAME
        and c.COLUMN_NAME = m.COLUMN_NAME
    where NOT REGEXP_LIKE(t.OWNER,
        'ANONYMOUS|PUBLIC|SYS|SYSTEM|DBSNMP|MDSYS|CTXSYS|XDB|TSMSYS|ORACLE.*|APEX.*|TEST?*|GG_.*|\$')
    %s /* extra excluded schema/owner */
    %s /* table filter */
    order by 1,2,4
    """ % (schema_exclusion_filter, table_name_filter)

    self.logger.debug(column_info_sql)
    curs_meta.execute(column_info_sql)

    rows = curs_meta.fetchall()
    self.logger.info("Fetched %d records of Oracle metadata" % curs_meta.rowcount)

    prev_table_key = ''
    for row in rows:
      current_table_key = "%s.%s" % (row[0], row[1])  # OWNER.TABLE_NAME
      if current_table_key != prev_table_key:
        self.table_dict[current_table_key] = {"partitioned": row[2]}
        prev_table_key = current_table_key

    curs_meta.close()
    return rows


  def get_extra_table_info(self):
    '''
    Index, Partition, Size info
    :return: index,partition,constraint
    '''
    index_info_sql = """
    select
      i.TABLE_OWNER, i.TABLE_NAME, i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS,
      t.CONSTRAINT_NAME,
      --LISTAGG(c.COLUMN_NAME,',')
      --  WITHIN GROUP (ORDER BY c.COLUMN_POSITION) as INDEX_COLUMNS,
      RTRIM(XMLAGG(xmlelement(s,c.COLUMN_NAME,',').extract('//text()')
            ORDER BY c.COLUMN_POSITION),',') INDEX_COLUMNS,
      COUNT(1) NUM_COLUMNS
    from ALL_INDEXES i
      join ALL_IND_COLUMNS c
        on i.OWNER = c.INDEX_OWNER
        and i.INDEX_NAME = c.INDEX_NAME
        and i.TABLE_OWNER = c.TABLE_OWNER
        and i.TABLE_NAME = c.TABLE_NAME
      left join (select coalesce(INDEX_OWNER,OWNER) OWNER, INDEX_NAME, CONSTRAINT_NAME
            from ALL_CONSTRAINTS t
            where INDEX_NAME IS NOT NULL) t
        on i.OWNER = t.OWNER
        and i.INDEX_NAME = t.INDEX_NAME
    group by i.TABLE_OWNER, i.TABLE_NAME, i.INDEX_NAME,
      i.INDEX_TYPE, i.UNIQUENESS, t.CONSTRAINT_NAME
    order by 1,2,3"""

    partition_col_sql = """
    select
      OWNER TABLE_OWNER, NAME TABLE_NAME,
      RTRIM(XMLAGG(xmlelement(s,c.COLUMN_NAME,',').extract('//text()')
        ORDER BY c.COLUMN_POSITION),',') PARTITION_COLUMNS,
      COUNT(1) NUM_COLUMNS
    from ALL_PART_KEY_COLUMNS c
    where c.OBJECT_TYPE = 'TABLE'
    group by c.OWNER, c.NAME
    order by 1,2"""

    curs_meta = self.conn_db.cursor()

    # get index and partition info one by one

    curs_meta.execute(partition_col_sql)
    rows = curs_meta.fetchall()
    for row in rows:
      table_name_key = "%s.%s" % (row[0], row[1])
      if table_name_key not in self.table_dict:
        continue

      self.table_dict[table_name_key]['partition_columns'] = row[2]
    self.logger.info("Found %d record for partition info" % curs_meta.rowcount)

    curs_meta.execute(index_info_sql)
    rows = curs_meta.fetchall()
    table_count = 0
    current_table_name = ''
    full_table_name = ''

    for row in rows:
      table_name_key = "%s.%s" % (row[0], row[1])
      if table_name_key not in self.table_dict:
        continue

      if "indexes" not in self.table_dict[table_name_key]:
        self.table_dict[table_name_key]["indexes"] = []

      self.table_dict[table_name_key]["indexes"].append(
        {
          "name": row[2],
          "type": row[3],
          "is_unique": 'Y' if row[4] == 'UNIQUE' else 'N',
          "constraint_name": row[5],
          "index_columns": row[6],
          "num_of_columns": row[7]
        }
      )
    self.logger.info("Found %d record for index info" % curs_meta.rowcount)


  def format_view_metadata(self, rows, schema):
    '''
    add view info from rows into schema
    note : view's original name is the same as full name
    :param rows:
    :return:
    '''
    self.logger.info("")


  def format_table_metadata(self, rows):
    '''
    add table info from rows into schema
    :param rows: input. each row is a database with all it's tables
    :param schema: {schema : _, type : _, tables : ['name' : _, ... 'original_name' : _] }
    :return:
    '''
    schema_dict = {"fields": []}
    table_record = {}
    table_idx = 0
    field_idx = 0

    for row in rows:
      table_name_key = "%s.%s" % (row[0], row[1])
      table_urn = "oracle:///%s/%s" % (row[0], row[1])

      if 'urn' not in table_record or table_urn != table_record['urn']:
        # This is a new table. Let's push the previous table record into output_list
        if 'urn' in table_record:
          schema_dict["num_fields"] = field_idx
          table_record["columns"] = json.dumps(schema_dict)
          self.table_output_list.append(table_record)

        properties = {
          "indexes": self.table_dict[table_name_key].get("indexes"),
          "partition_column": self.table_dict[table_name_key].get("partition_column")
        }
        table_record = {
          "name": row[1],
          "columns": None,
          "schema_type": "JSON",
          "properties": json.dumps(properties),
          "urn": table_urn,
          "source": "Oracle",
          "location_prefix": row[0],
          "parent_name": row[0],
          "storage_type": "Table",
          "dataset_type": "oracle",
          "is_partitioned": 'Y' if self.table_dict[table_name_key]["partitioned"] == 'YES' else 'N'
        }
        schema_dict = {"fields": []}
        table_idx += 1
        field_idx = 0

      field_record = {
        "sort_id": self.num_to_int(row[3]),
        "name": row[4],
        "data_type": row[5],
        "nullable": row[6],
        "size": self.num_to_int(row[7]),
        "precision": self.num_to_int(row[8]),
        "scale": self.num_to_int(row[9]),
        "default_value": self.trim_newline(row[12]),
        "doc": self.trim_newline(row[13])
      }
      schema_dict['fields'].append(field_record)
      field_record['dataset_urn'] = table_urn
      self.field_output_list.append(field_record)
      field_idx += 1

    # finish all remaining rows
    schema_dict["num_fields"] = field_idx
    table_record["columns"] = json.dumps(schema_dict)
    self.table_output_list.append(table_record)
    self.logger.info("%d Table records generated" % table_idx)


  def get_sample_data(self, database_name, table_name):
    """
    find the reference dataset (if it has), select top 10 from teradata
    :return: (reference_urn, json of sample data)
    """
    fullname = ''
    columns = []
    rows_data = []

    sql = 'SELECT * FROM %s WHERE ROWNUM<=10' % fullname
    curs_td = self.conn_db.cursor()
    rows = []
    try:
      curs_td.execute(sql)
      rows = curs_td.fetchall()
      for i, value in enumerate(rows[0]):
        columns.append(curs_td.description[i][0])
      for r in rows:
        row_data = []
        # encode each field to a new value
        for i, value in enumerate(r):
          new_value = unicode(value, errors='ignore')
          if isinstance(value, bytearray):
            new_value = ''.join(format(x, '02x') for x in value)
          elif value is None:
            new_value = ''
          row_data.append(new_value)
        rows_data.append(row_data)
    except Exception, e:
      self.logger.error('sql : ' + sql)
      if len(rows) == 0:
        self.logger.error("dataset {0} is empty".format(fullname))
      else:
        self.logger.error("dataset {0} is not accessible.".format(fullname))
        self.logger.error('result : ' + str(rows))
        self.logger.error(e)
      pass

    ref_urn = 'oracle:///' + fullname.replace('.', '/').replace('"', '')
    data_with_column = map(lambda x: dict(zip(columns, x)), rows_data)
    return ref_urn, json.dumps({'sample': data_with_column})


  def num_to_int(self, num):
    try:
      return int(num)
    except (ValueError, TypeError):
      return None

  def trim_newline(self, line):
    return None if line is None else line.replace('\n', ' ').replace('\r', ' ').encode('ascii', 'ignore')

  def write_csv(self, csv_filename, csv_columns, data_list):
    csvfile = open(csv_filename, 'wb')
    os.chmod(csv_filename, 0644)
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\0')
    writer.writeheader()
    for data in data_list:
      writer.writerow(data)
    csvfile.close()


  def run(self, database_name, table_name, table_output_file, field_output_file, sample_output_file, sample=False):
    """
    The entrance of the class, extract schema and sample data
    Notice the database need to have a order that the databases have more info (DWH_STG) should be scaned first.
    :param database_name:
    :param table_name:
    :param table_output_file:
    :param filed_output_file:
    :param sample_output_file:
    :param collect_sample:
    :return:
    """
    if database_name is None and table_name is None:  # default route: process everything
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      # table info
      rows = self.get_table_info(None, None)
      self.get_extra_table_info()
      self.format_table_metadata(rows)
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Collecting table info [%s -> %s]" % (str(begin), str(end)))

      csv_columns = ['name', 'columns', 'schema_type', 'properties', 'urn', 'source', 'location_prefix',
                     'parent_name', 'storage_type', 'dataset_type', 'is_partitioned']
      self.write_csv(table_output_file, csv_columns, self.table_output_list)

      csv_columns = ['dataset_urn', 'sort_id', 'name', 'data_type', 'nullable',
                     'size', 'precision', 'scale', 'default_value', 'doc']
      self.write_csv(field_output_file, csv_columns, self.field_output_list)

    if sample:
      csvfile = open(sample_output_file, 'wb')
      os.chmod(sample_output_file, 0666)
      writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter='\x1A', lineterminator='\n',
                              quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\0')
      self.logger.info("Writing to CSV file {}".format(sample_output_file))

      # collect sample data
      for onedatabase in schema:
        database_name = onedatabase['database']
        if 'tables' in onedatabase:
          alltables = onedatabase['tables']
        else:
          alltables = onedatabase['views']

        for onetable in alltables:
          table_name = onetable['original_name'].split('.')[1]
        if table_name in scaned_dict:
          sample_record = SampleDataRecord('oracle', '/' + database_name + '/' + table_name,
                                           scaned_dict[table_name]['ref_urn'], scaned_dict[table_name]['data'])
        else:
          (ref_urn, sample_data) = self.get_sample_data(database_name, table_name)
          sample_record = SampleDataRecord('oracle', '/' + database_name + '/' + table_name, '', sample_data)
          scaned_dict[table_name] = {'ref_urn': ref_urn, 'data': sample_data}
        writer.writerow(sample_record)
      csvfile.close()


if __name__ == "__main__":
  args = sys.argv[1]

  # connection
  username = args[Constant.ORA_DB_USERNAME_KEY]
  password = args[Constant.ORA_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.ORA_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.ORA_DB_URL_KEY]

  e = OracleExtract()
  e.conn_db = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
  collect_sample = False
  if Constant.ORA_LOAD_SAMPLE in args:
    collect_sample = bool(args[Constant.ORA_LOAD_SAMPLE])
  e.databases = args[Constant.ORA_EXCLUDE_DATABASES_KEY].split(',')

  try:
    e.conn_db.cursor().execute("ALTER SESSION SET TIME_ZONE = 'US/Pacific'")
    e.conn_db.cursor().execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    e.conn_db.cursor().execute("CALL dbms_application_info.set_module('%s','%d')" %
                               ('WhereHows (Jython)', os.getpid()))
    e.conn_db.commit()

    e.run(None, None,
          args[Constant.ORA_SCHEMA_OUTPUT_KEY],
          args[Constant.ORA_FIELD_OUTPUT_KEY],
          args[Constant.ORA_SAMPLE_OUTPUT_KEY],
          sample=collect_sample)
  finally:
    e.conn_db.cursor().close()
    e.conn_db.close()
