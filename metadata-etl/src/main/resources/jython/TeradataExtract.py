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
import sys, os, re, json
import datetime
from distutils.util import strtobool
from wherehows.common.schemas import SampleDataRecord
from wherehows.common.writers import FileWriter
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class TeradataExtract:
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def get_view_info(self, database_name, view_name):
    '''
    :param database_name:
    :param view_name:
    :return:
    '''
    view_cols = []
    curs_vw = self.conn_td.cursor()
    view_sql = '''
        SELECT Trim(DatabaseName) DatabaseName, Trim(TableName) TableName, RequestText,
               CreateTimestamp(CHAR(19)), LastAlterTimestamp(CHAR(19)), AccessCount
        FROM   DBC.Tables
        WHERE  DatabaseName = '%s'
        AND    TableName NOT LIKE ALL ('!_%%', '#%%', 'TMP%%', 'TEMP%%') ESCAPE '!'
        AND    TableKind = 'V' ORDER BY 2''' % database_name

    if not view_name is None:
      view_sql = view_sql + ''' AND TableName = '%s' ''' % view_name

    curs_vw.execute(view_sql)
    views = curs_vw.fetchall()

    for vw in views:
      try:
        # table_sql = 'CREATE VOLATILE TABLE vt_get_column_info AS (SELECT * FROM %s.%s) WITH NO DATA NO PRIMARY INDEX' % (vw[0], vw[1])
        help_column_sql = 'help column %s.%s.*' % (vw[0], vw[1])
        curs_vw.execute(help_column_sql)
        rows = curs_vw.fetchall()
      except Exception, e:
        # print str(e), vw[1], len(views)
        continue
      for r in rows:
        column_name = r[0].strip()
        data_type = r[1].strip()
        nullable = r[2].strip()
        format = r[3].strip()
        max_length = r[4] & 0xffff
        decimal_total_digits = r[5] & 0xffff if r[5] else None
        decimal_fractional_digits = r[6] & 0xffff if r[6] else (0 if r[5] else None)
        char_type = r[16] & 0xffff if r[16] else None
        if data_type == 'I1':
          data_type = 'BYTEINT'
        elif data_type == 'I2':
          data_type = 'SMALLINT'
        elif data_type == 'I':
          data_type = 'INT'
        elif data_type == 'F':
          data_type = 'FLOAT'
        elif data_type == 'I8':
          data_type = 'BIGINT'
        elif data_type == 'DA':
          data_type = 'DATE'
        elif data_type == 'AT':
          data_type = 'TIME'
        elif data_type == 'TS':
          data_type = 'TIMESTAMP'
        elif data_type == 'SZ':
          data_type = 'TIMESTAMP WITH TIMEZONE'
        elif data_type == 'TZ':
          data_type = 'TIME WITH TIMEZONE'
        elif data_type == 'CO':
          data_type = 'CLOB'
        elif data_type == 'CF':
          data_type = 'CHAR(' + str(max_length / char_type) + ')'
        elif data_type == 'CV':
          data_type = 'VARCHAR(' + str(max_length / char_type) + ')'
        elif data_type == 'BV':
          data_type = 'VARBYTE(' + str(max_length) + ')'
        elif data_type == 'D':
          data_type = 'DECIMAL(' + str(decimal_total_digits) + ',' + str(decimal_fractional_digits) + ')'

        if data_type not in ['DATE', 'TIME', 'TIMESTAMP', 'TIME WITH TIMEZONE', 'TIMESTAMP WITH TIMEZONE']:
          format = ''

        view_cols.append((
          vw[0], vw[1], vw[2], str(vw[3]), str(vw[4]), vw[5], column_name, format, nullable, data_type, max_length,
          decimal_total_digits, decimal_fractional_digits))
    curs_vw.close()
    return view_cols

  def get_table_info(self, database_name, table_name):
    '''
    get table, column info from teradata DBC.Tables
    :param database_name:
    :param table_name: not used in common case
    :return:
    '''
    td_table_name_filter = ''
    if not database_name is None:
      td_database_name = database_name
    elif len(table_name) > 0:
      if table_name.find('.') > 0:
        (td_database_name, td_table_name) = table_name.split('.')
      else:
        td_database_name = self.default_database
        td_table_name = table_name
      td_table_name_filter = "AND    a.TableName = '%s' " % td_table_name

    curs_td = self.conn_td.cursor()

    col_stats_sql = """SELECT c.DatabaseName,
           c.TableName2 TableName,
           c.CreateTimestamp (FORMAT 'YYYY-MM-DDBHH:MI:SS') (CHAR(19)) createTimestamp,
           c.LastAlterTimestamp (CHAR(19)) TableLastAlterTimestamp,
           c.LastAccessTimestamp (FORMAT 'YYYY-MM-DDBHH:MI:SS') (CHAR(19)) LastAccessTimestamp,
           c.TableAccessCount,
           RTRIM(a.columnname) ColumnName,
           CASE
              WHEN a.columntype IN ('DA','AT','TS','SZ','TZ') THEN
                 RTrim(a.ColumnFormat)
              ELSE
                 NULL
           END ColumnFormat,
           a.DefaultValue,
           a.nullable,
           a.LastAccessTimestamp (FORMAT 'YYYY-MM-DDBHH:MI:SS') (CHAR(19)) LastAccessTimestamp,
           a.AccessCount,
           b.UniqueValueCount (bigint) UniqueValueCount,
           b.LastCollectTimestamp (FORMAT 'YYYY-MM-DDBHH:MI:SS') (CHAR(19)) LastCollectTimestamp,
           CASE
              WHEN a.columntype = 'I1' THEN 'BYTEINT'
              WHEN a.columntype = 'I2' THEN 'SMALLINT'
              WHEN a.columntype = 'I'  THEN 'INT'
              WHEN a.columntype = 'F'  THEN 'FLOAT'
              WHEN a.columntype = 'I8' THEN 'BIGINT'
              WHEN a.columntype = 'DA' THEN 'DATE'
              WHEN a.columntype = 'AT' THEN 'TIME'
              WHEN a.columntype = 'TS' THEN 'TIMESTAMP'
              WHEN a.columntype = 'SZ' THEN 'TIMESTAMP WITH TIMEZONE'
              WHEN a.columntype = 'TZ' THEN 'TIME WITH TIMEZONE'
              WHEN a.columntype = 'CO' THEN 'CLOB'
              WHEN a.columntype = 'BV' THEN 'VARBYTE(' || ColumnLength || ')'
              WHEN a.columntype = 'CF' THEN 'CHAR('|| TRIM(a.ColumnLength/a.CharType) || ')'
              WHEN a.columntype = 'CV' THEN 'VARCHAR('|| TRIM(a.ColumnLength/a.CharType) || ')'
              WHEN a.columntype = 'D'  THEN 'DECIMAL(' || TRIM(a.DecimalTotalDigits) || ',' || TRIM(a.DecimalFractionalDigits) || ')'
           END Data_Type,
           a.ColumnLength,
           a.DecimalTotalDigits,
           a.DecimalFractionalDigits,
           a.ColumnId Column_Id,
           RTrim(c.CreatorName) CreatorName,
           RTrim(c.TableName) OriginalTableName
    FROM   (
    select RTrim(a.DatabaseName) DatabaseName,
           case when regexp_similar(a.tableName, '[[:alnum:]_]+[[:digit:]]{8}([^[:digit:]]|$).*', 'c') = 1 then
                case when regexp_substr(a.TableName, '([[:digit:]]{8})([^[:digit:]]|$)') between '20000101' and '20991231'
                     then rtrim(regexp_replace(a.tableName, '([[:digit:]]{8})', '${YYYYMMDD}', 1, 1, 'c'))
                     when regexp_substr(a.TableName, '([[:digit:]]{8})([^[:digit:]]|$)') between '01012000' and '12312099'
                     then rtrim(regexp_replace(a.tableName, '([[:digit:]]{8})', '${MMDDYYYY}', 1, 1, 'c'))
                     else RTRIM(a.tablename)
                end

                when regexp_similar(a.tableName, '[[:alnum:]_]+[[:digit:]]{4}_[[:digit:]]{2}([^[:digit:]]|$).*', 'c') = 1
                     and regexp_substr(a.TableName, '([[:digit:]]{4})_[[:digit:]]{2}([^[:digit:]]|$)') between '2000_01' and '9999_12'
                then rtrim(regexp_replace(a.tableName, '([[:digit:]]{4}_[[:digit:]]{2})', '${YYYY_MM}', 1, 1, 'c'))

                when regexp_similar(a.tableName, '[[:alnum:]_]+[[:digit:]]{6}([^[:digit:]]|$).*', 'c') = 1
                     and regexp_substr(a.TableName, '([[:digit:]]{6})([^[:digit:]]|$)') between '200001' and '999912'
                then rtrim(regexp_replace(a.tableName, '([[:digit:]]{6})', '${YYYYMM}', 1, 1, 'c'))

                else RTRIM(a.tablename)
           end TableName2,
           a.TableName,
           a.CreateTimestamp,
           a.LastAlterTimestamp,
           a.LastAccessTimestamp,
           a.AccessCount TableAccessCount,
           a.CreatorName
    from DBC.Tables a where a.TableKind = 'T'
    AND  a.DatabaseName = '%s'
    %s
    AND  case when a.DatabaseName in ('DM_BIZ', 'DM_SEC') and a.AccessCount < 200 then -1
              when a.DatabaseName in ('DM_DA', 'DM_DSC', 'NCRM') and a.AccessCount < 100 then -1
              -- else a.AccessCount
              else 1 -- include all other tables
         end >= 1
    AND  a.TableName NOT LIKE ALL ('INFA%%', 'tmp!_%%', 'temp!_%%', 'ut!_%%', '!_%%', '#%%' 'ET!_%%', 'LS!_%%', 'VT!_%%', 'LOGTABLE%%', 'backup%%', 'bkp%%', 'W!_%%') ESCAPE '!'
    AND  RTRIM(a.TableName) NOT LIKE ALL ('%%!_tmp', '%%!_temp', '%%!_ERR!_.', '%%!_bkp', '%%!_backup') ESCAPE '!'
    AND  REGEXP_SIMILAR(RTRIM(a.TableName), '.*_tmp_[0-9]+','i') = 0
    AND  REGEXP_SIMILAR(RTRIM(a.TableName), '.*_tmp[0-9]+','i') = 0
    QUALIFY RANK() OVER (PARTITION BY DatabaseName, TableName2 ORDER BY a.TableName desc) = 1
           ) c
           JOIN
           DBC.Columns a
    ON     (c.databasename = a.databasename AND
            c.tablename = a.tablename)
           LEFT OUTER JOIN
           DBC.ColumnStatsV b
    ON     (a.databasename = b.databasename AND
            a.tablename = b.tablename AND
            a.columnname = b.columnname)
    ORDER BY 1, 2, a.ColumnId """ % (td_database_name, td_table_name_filter)

    curs_td.execute(col_stats_sql)

    rows = curs_td.fetchall()

    curs_td.close()

    return rows

  def get_extra_table_info(self, database_name):
    '''
    Index, Partition, Size info
    :param database_name:
    :return: size, partition, indice
    '''
    table_size_sql = """select RTrim(TableName), cast(sum(CurrentPerm)/1024/1024 as BIGINT) size_in_mb
        from DBC.TableSize where DatabaseName = '%s'
        AND TableName NOT LIKE ALL ('INFA%%', 'tmp!_%%', 'temp!_%%', 'ut!_%%', '!_%%', '#%%' 'ET!_%%', 'LS!_%%', 'VT!_%%', 'LOGTABLE%%', 'backup%%', 'bkp%%', 'W!_%%') ESCAPE '!'
        AND  RTRIM(TableName) NOT LIKE ALL ('%%!_tmp', '%%!_temp', '%%!_ERR!_.', '%%!_bkp', '%%!_backup') ESCAPE '!'
        AND  REGEXP_SIMILAR(RTRIM(TableName), '.*_tmp_[0-9]+','i') = 0
        AND  REGEXP_SIMILAR(RTRIM(TableName), '.*_tmp[0-9]+','i') = 0
        group by 1 order by 1 """ % (database_name)

    table_index_sql = """select RTrim(TableName), IndexNumber, IndexType, UniqueFlag, IndexName, RTrim(ColumnName), ColumnPosition, AccessCount
        from DBC.Indices where DatabaseName = '%s' order by TableName, IndexNumber, ColumnPosition""" % (database_name)

    table_partition_sql = """select RTrim(TableName), ConstraintText
        from DBC.IndexConstraints where DatabaseName = '%s' and ConstraintType = 'Q'
        order by TableName""" % (database_name)

    extra_table_info = {}

    curs_td = self.conn_td.cursor()
    # curs_td.execute("SET QUERY_BAND = 'script=%s; pid=%d; hostname=%s; task=extra_table_info;' FOR SESSION" % (os.path.basename(__file__), os.getpid(), os.getenv('HOSTNAME')))

    # get size index and partition info one by one
    curs_td.execute(table_size_sql)
    rows = curs_td.fetchall()
    for row in rows:
      full_table_name = database_name + '.' + row[0]
      extra_table_info[full_table_name] = {'size_in_mb': row[1], 'partitions': [], 'indices': []}

    curs_td.execute(table_partition_sql)
    rows = curs_td.fetchall()
    for row in rows:
      full_table_name = database_name + '.' + row[0]
      if full_table_name not in extra_table_info:
        continue

      search_result = re.search('CHECK \(/\*([0-9]+)\*/ (.*)\)$', row[1], re.IGNORECASE)
      partition_level = 1
      if search_result:
        partition_level = int(search_result.group(1))
        partition_info = search_result.group(2).replace("\r", "").replace(") IS NOT NULL AND ", ")\n").replace(
          ") IS NOT NULL", ")")
        extra_table_info[full_table_name]['partitions'] = partition_info.split("\n")

      search_result = re.search('CHECK \(\((.*)\) BETWEEN [0-9]+ AND [0-9]+\)$', row[1], re.IGNORECASE)
      if search_result:
        partition_info = search_result.group(1).replace("\r", "")
        extra_table_info[full_table_name]['partitions'] = [partition_info]

    curs_td.execute(table_index_sql)
    rows = curs_td.fetchall()
    table_count = 0
    current_table_name = ''
    full_table_name = ''

    for row in rows:
      if current_table_name <> row[0]:
        if table_count > 0:
          # finish previous table's last index
          indices[-1]['column_list'] = column_list
          if full_table_name in extra_table_info:
            extra_table_info[full_table_name]['indices'] = indices

        full_table_name = database_name + '.' + row[0]
        if full_table_name not in extra_table_info:
          continue

        table_count += 1
        current_table_name = row[0]
        current_index_number = 0
        indices = []

      if current_index_number <> row[1]:
        if current_index_number > 0:
          indices[-1]['column_list'] = column_list
        # new index
        current_index_number = row[1]
        indices.append(
          {'index_number': row[1], 'index_type': index_type[row[2]], 'is_unique': row[3], 'index_name': row[4],
           'access_count': row[7], 'column_list': ''})
        column_list = row[5]
      else:
        column_list += ", %s" % row[5]

    if len(indices) > 0:
      indices[-1]['column_list'] = column_list
      if full_table_name in extra_table_info:
        extra_table_info[full_table_name]['indices'] = indices

    return extra_table_info

  def format_view_metadata(self, rows, schema):
    '''
    add view info from rows into schema
    note : view's original name is the same as full name
    :param rows:
    :param schema:
    :return:
    '''
    db_dict = {}
    table_dict = {}
    for row in rows:
      if row[0] not in db_dict:
        schema.append({'database': row[0], 'type': 'Teradata', 'views': []})
        db_dict[row[0]] = len(schema) - 1
      db_idx = db_dict[row[0]]
      full_name = row[0] + '.' + row[1]
      ref_table_list = []
      ref_table_list = set(re.findall(r"\s+FROM\s+(\w+\.\w+)[\s,;]", row[2], re.DOTALL | re.IGNORECASE))
      search_result = set(re.findall(r"\s+JOIN\s+(\w+\.\w+)[\s,;]", row[2], re.DOTALL | re.IGNORECASE))
      ref_table_list = list(set(ref_table_list) | set(search_result))

      if full_name not in table_dict:
        schema[db_idx]['views'].append(
          {'name': row[1], 'type': 'View', 'createTime': row[3], 'lastAlterTime': row[4], 'accessCount': row[5],
           'referenceTables': ref_table_list, 'viewSqlText': row[2].replace("\r", "\n"), 'columns': [],
           'original_name': full_name})
        table_dict[full_name] = len(schema[db_idx]['views']) - 1
      table_idx = table_dict[full_name]
      schema[db_idx]['views'][table_idx]['columns'].append(
        {'name': row[6], 'nullable': row[8], 'dataType': row[9], 'maxByteLength': row[10], 'precision': row[11],
         'scale': row[12]})
      column_idx = len(schema[db_idx]['views'][table_idx]['columns']) - 1
      if row[7]:
        schema[db_idx]['views'][table_idx]['columns'][column_idx]['columnFormat'] = row[7].strip()

    self.logger.info("%s %6d  views with %6d columns processed for %12s" % (
      datetime.datetime.now(), table_idx + 1, len(rows), row[0]))

  def format_table_metadata(self, rows, schema):
    '''
    add table info from rows into schema
    :param rows: input. each row is a database with all it's tables
    :param schema: {database : _, type : _, tables : ['name' : _, ... 'original_name' : _] }
    :return:
    '''
    db_dict = {}
    table_dict = {}
    db_idx = len(schema) - 1
    table_idx = -1
    for row in rows:
      if row[0] not in db_dict:
        schema.append({'database': row[0], 'type': 'Teradata', 'tables': []})
        db_idx += 1
        db_dict[row[0]] = db_idx
        extra_table_info = self.get_extra_table_info(row[0])
      full_name = ''
      if row[0]:
        full_name = row[0]
        if row[1]:
          full_name += '.' + row[1]
      elif row[1]:
        full_name = row[1]
      # full_name = row[0] + '.' + row[1]
      original_name = row[0] + '.' + row[20]
      if original_name not in extra_table_info:
        self.logger.error('ERROR : {0} not in extra_table_info!'.format(original_name))
        continue
      if full_name not in table_dict:
        schema[db_idx]['tables'].append(
          {'name': row[1], 'type': 'Table', 'createTime': row[2], 'lastAlterTime': row[3], 'lastAccessTime': row[4],
           'accessCount': row[5], 'owner': row[19], 'sizeInMbytes': extra_table_info[original_name]['size_in_mb'],
           'partitions': extra_table_info[original_name]['partitions'],
           'indices': extra_table_info[original_name]['indices'], 'columns': [], 'original_name': original_name})
        table_idx += 1
        table_dict[full_name] = table_idx
        # print "%6d: %s: %s" % (table_idx, full_name, str(schema[db_idx]['tables'][table_idx]))

      schema[db_idx]['tables'][table_idx]['columns'].append(
        {'name': row[6], 'nullable': row[9], 'lastAccessTime': row[8],
         'accessCount': row[11] & 0xffff if row[11] else None, 'dataType': row[14] if row[14] else 'N/A',
         'maxByteLength': row[15] & 0xffff, 'precision': row[16] & 0xffff if row[16] else None,
         'scale': row[17] & 0xffff if row[17] else None})
      column_idx = len(schema[db_idx]['tables'][table_idx]['columns']) - 1
      if not row[8] is None:
        schema[db_idx]['tables'][table_idx]['columns'][column_idx]['defaultValue'] = row[8]
      if not row[7] is None:
        schema[db_idx]['tables'][table_idx]['columns'][column_idx]['columnFormat'] = row[7].strip()
      if not row[12] is None:
        schema[db_idx]['tables'][table_idx]['columns'][column_idx]['statistics'] = {
          'uniqueValueCount': row[12] & 0xffff, 'lastStatsCollectTime': str(row[13])}

    self.logger.info("%s %6d tables with %6d columns processed for %12s" % (
      datetime.datetime.now(), table_idx + 1, len(rows), row[0]))

  def get_sample_data(self, database_name, table_name):
    """
    find the reference dataset (if it has), select top 10 from teradata
    :return: (reference_urn, json of sample data)
    """
    fullname = ''
    columns = []
    rows_data = []
    # doesn't have permission for these databases, fetch sample from DWH_STG's correspond tables
    if database_name in ['DWH_DIM', 'DWH_FACT', 'DWH_TRK', 'DWH_AGG', 'DWH_CPY', 'DWH_MSTR', 'DWH_SEC']:
      fullname = 'DWH_STG."' + table_name + '"'
    else:
      fullname = database_name + '."' + table_name + '"'

    sql = 'LOCK ROW FOR ACCESS SELECT top 10 * FROM ' + fullname
    curs_td = self.conn_td.cursor()
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

    ref_urn = 'teradata:///' + fullname.replace('.', '/').replace('"', '')
    data_with_column = map(lambda x:dict(zip(columns, x)), rows_data)
    return ref_urn, json.dumps({'sample': data_with_column})

  def run(self, database_name, table_name, schema_output_file, sample_output_file, sample=True):
    """
    The entrance of the class, extract schema and sample data
    Notice the database need to have a order that the databases have more info (DWH_STG) should be scaned first.
    :param database_name:
    :param table_name:
    :param schema_output_file:
    :return:
    """
    cur = self.conn_td.cursor()
    schema = []

    f_log = open(self.log_file, "a")

    schema_json = open(schema_output_file, 'wb')
    os.chmod(schema_output_file, 0666)


    if database_name is None and table_name is None:  # default route: process everything
      for database_name in self.databases:
        self.logger.info("Collecting tables in database : " + database_name)
        # table info
        rows = []
        begin = datetime.datetime.now().strftime("%H:%M:%S")
        rows.extend(self.get_table_info(database_name, table_name))
        if len(rows) > 0:
          self.format_table_metadata(rows, schema)
        end = datetime.datetime.now().strftime("%H:%M:%S")
        f_log.write("Get table info %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

        # view info
        rows = []
        begin = datetime.datetime.now().strftime("%H:%M:%S")
        rows.extend(self.get_view_info(database_name, table_name))
        if len(rows) > 0:
          self.format_view_metadata(rows, schema)
        end = datetime.datetime.now().strftime("%H:%M:%S")
        f_log.write("Get view  info %12s [%s -> %s]\n" % (database_name, str(begin), str(end)))

      scaned_dict = {}  # a cache of {name : {urn : _, data : _}} to avoid repeat computing

      if sample:
        self.logger.info("Start collecting sample data.")
        open(sample_output_file, 'wb')
        os.chmod(sample_output_file, 0666)
        sample_file_writer = FileWriter(sample_output_file)

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
              sample_record = SampleDataRecord('teradata', '/' + database_name + '/' + table_name,
                                               scaned_dict[table_name]['ref_urn'], scaned_dict[table_name]['data'])
            else:
              (ref_urn, sample_data) = self.get_sample_data(database_name, table_name)
              sample_record = SampleDataRecord('teradata', '/' + database_name + '/' + table_name, '', sample_data)
              scaned_dict[table_name] = {'ref_urn': ref_urn, 'data': sample_data}
            sample_file_writer.append(sample_record)
        sample_file_writer.close()

    # print 'byte size of schema : ' + str(sys.getsizeof(schema))
    schema_json.write(json.dumps(schema, indent=None) + '\n')
    cur.close()
    schema_json.close()
    f_log.close()


if __name__ == "__main__":
  args = sys.argv[1]

  # connection
  username = args[Constant.TD_DB_USERNAME_KEY]
  password = args[Constant.TD_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.TD_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.TD_DB_URL_KEY]

  e = TeradataExtract()
  e.conn_td = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
  do_sample = False
  if Constant.TD_LOAD_SAMPLE in args:
    do_sample = strtobool(args[Constant.TD_LOAD_SAMPLE])

  if datetime.datetime.now().strftime('%a') not in args[Constant.TD_COLLECT_SAMPLE_DATA_DAYS]:
    do_sample = False

  try:
    e.conn_td.cursor().execute(
      "SET QUERY_BAND = 'script=%s; pid=%d; ' FOR SESSION;" % ('TeradataExtract.py', os.getpid()))
    e.conn_td.commit()
    e.log_file = args[Constant.TD_LOG_KEY]
    e.databases = args[Constant.TD_TARGET_DATABASES_KEY].split(',')
    e.default_database = args[Constant.TD_DEFAULT_DATABASE_KEY]
    index_type = {'P': 'Primary Index', 'K': 'Primary Key', 'S': 'Secondary Index', 'Q': 'Partitioned Primary Index',
                  'J': 'Join Index', 'U': 'Unique Index'}

    e.run(None, None, args[Constant.TD_SCHEMA_OUTPUT_KEY], args[Constant.TD_SAMPLE_OUTPUT_KEY], sample=do_sample)
  finally:
    e.conn_td.close()

