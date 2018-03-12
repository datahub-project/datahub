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
import datetime
import FileUtil
import json
import os
import sys

from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
from wherehows.common import Constant


class RedshiftExtract:
  """
  Extract Redshift data, store it in a JSON file
  """
  table_dict = {}
  table_output_list = []
  field_output_list = []
  sample_output_list = []


  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def get_table_info(self):
    """
    get table, column info from Oracle all_tables
    here Owner, Schema, Database have same meaning: a collection of tables
    :param excluded_owner_list: schema blacklist
    :param table_name: get specific table name, not used in common case
    :return:
    """
    curs_meta = self.conn_db.cursor()

    column_info_sql = """
      SELECT c.table_schema, 
          c.table_name, 
          c.column_name, 
          c.column_default, 
          c.is_nullable, 
          c.data_type, 
          c.character_maximum_length, 
          c.numeric_precision,
          CASE WHEN constraint_type = 'PRIMARY KEY' THEN true ELSE false END,
          table_type
      FROM information_schema.tables AS t
      INNER JOIN information_schema.columns AS c ON t.table_name = c.table_name AND c.table_schema = t.table_schema
      LEFT JOIN -- find the primary keys and join back on column
        (SELECT 
          tc.table_schema, 
          tc.table_name, 
          kc.column_name, 
          constraint_type
        FROM information_schema.table_constraints tc
        INNER JOIN information_schema.key_column_usage kc 
            ON kc.table_name = tc.table_name AND kc.table_schema = tc.table_schema AND kc.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
        ) AS primary_key_info
       ON primary_key_info.table_schema=c.table_schema AND primary_key_info.table_name = c.table_name 
        AND primary_key_info.column_name = c.column_name 
      WHERE c.table_schema = 'warehouse'
      ORDER BY ordinal_position;
      """

    self.logger.debug(column_info_sql)
    curs_meta.execute(column_info_sql)

    rows = curs_meta.fetchall()
    self.logger.info("Fetched %d records of Redshift metadata" % len(rows))
    curs_meta.close()

    prev_table_key = ''
    for row in rows:
      current_table_key = "%s.%s" % (row[0], row[1]) 
      if current_table_key != prev_table_key:
        self.table_dict[current_table_key] = {"primary_key": row[8]}. # Update the constriant type
        prev_table_key = current_table_key

    self.logger.info("Fetched %d tables: %s" % (len(self.table_dict), self.table_dict))
    return rows

  def format_table_metadata(self, rows):
    """
    add table info with columns from rows into table schema
    :param rows: input. each row is a table column
    :param schema: {schema : _, type : _, tables : ['name' : _, ... 'original_name' : _] }
    :return:
    """
    schema_dict = {"fields": []}
    table_record = {}
    table_idx = 0
    field_idx = 0

    for row in rows:
      table_name_key = "%s.%s" % (row[0], row[1])
      table_urn = "redshift:///%s/%s" % (row[0], row[1])

      if 'urn' not in table_record or table_urn != table_record['urn']:
        # This is a new table. Let's push the previous table record into output_list
        if 'urn' in table_record:
          schema_dict["num_fields"] = field_idx
          table_record["columns"] = json.dumps(schema_dict)
          self.table_output_list.append(table_record)

        properties = {
          "primary_key": self.table_dict[table_name_key].get("primary_key"),
        }
        table_record = {
          "name": row[1],
          "columns": None,
          "schema_type": "JSON",
          "properties": json.dumps(properties),
          "urn": table_urn,
          "source": "Redshift",
          "location_prefix": row[0],
          "parent_name": row[0],
          "storage_type": row[9],
          "dataset_type": "Redshift"
        }
        schema_dict = {"fields": []}
        table_idx += 1
        field_idx = 0

      field_record = {
        "name": row[3],
        "data_type": row[5],
        "nullable": row[4],
        "size": self.num_to_int(row[6]),
        "precision": self.num_to_int(row[7]),
        "default_value": self.trim_newline(row[3])
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

  def num_to_int(self, num):
    try:
      return int(num)
    except (ValueError, TypeError):
      return None

  def trim_newline(self, line):
    return line.replace('\n', ' ').replace('\r', ' ').strip().encode('ascii', 'ignore') if line else None

  def write_csv(self, csv_filename, csv_columns, data_list):
    csvfile = open(csv_filename, 'wb')
    os.chmod(csv_filename, 0644)
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\0')
    writer.writeheader()
    for data in data_list:
      writer.writerow(data)
    csvfile.close()

  def run(self, exclude_database_list, table_name, table_file, field_file, sample_file, sample=False):
    """
    The entrance of the class, extract schema and sample data
    Notice the database need to have a order that the databases have more info (DWH_STG) should be scaned first.
    :param exclude_database_list: list of excluded databases/owners/schemas
    :param table_name: specific table name to query
    :param table_file: table output csv file path
    :param field_file: table fields output csv file path
    :param sample_file: sample data output csv file path
    :param sample: do sample or not
    :return:
    """
    if database_name is None and table_name is None:  # default route: process everything
      begin = datetime.datetime.now().strftime("%H:%M:%S")
      # collect table info
      rows = self.get_table_info()
      self.get_extra_table_info()
      self.format_table_metadata(rows)
      mid = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Collecting table info [%s -> %s]" % (str(begin), str(mid)))

      csv_columns = ['name', 'columns', 'schema_type', 'properties', 'urn', 'source', 'location_prefix',
                     'parent_name', 'storage_type', 'dataset_type']
      self.write_csv(table_file, csv_columns, self.table_output_list)

      csv_columns = ['dataset_urn', 'name', 'data_type', 'nullable',
                     'size', 'precision', 'default_value']
      self.write_csv(field_file, csv_columns, self.field_output_list)

    if sample:  # no sample data yet
      None


if __name__ == "__main__":
  args = sys.argv[1]

  # connection
  username = args[Constant.RED_DB_USERNAME_KEY]
  password = args[Constant.RED_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.RED_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.RED_DB_URL_KEY]

  e = RedshiftExtract()
  e.conn_db = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)

  
  collect_sample = False

  try:
      self.logger.info("Running extract")
      self.run(None, None,
            args[Constant.RED_SCHEMA_OUTPUT_KEY],
            args[Constant.RED_FIELD_OUTPUT_KEY],
            args[Constant.SRED_SAMPLE_OUTPUT_KEY],
            sample=collect_sample)
  except Exception as ex:
      self.logger.info("Error in Redshift Extract")
      self.logger.info(ex)
      raise
  finally:
      self.conn_db.close()
