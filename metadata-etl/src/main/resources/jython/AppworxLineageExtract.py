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

from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
import ConfigParser, os
import DbUtil
from pyparsing import *
import sys
import hashlib
import gzip
import StringIO
import json
import datetime
import time
import re
from org.slf4j import LoggerFactory
from AppworxLogParser import AppworxLogParser
from PigLogParser import PigLogParser
from BteqLogParser import BteqLogParser
import ParseUtil

class AppworxLineageExtract:

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.aw_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.aw_cursor = self.aw_con.cursor()
    self.remote_hadoop_script_dir = args[Constant.AW_REMOTE_HADOOP_SCRIPT_DIR]
    self.local_script_path = args[Constant.AW_LOCAL_SCRIPT_PATH]
    self.remote_script_path = args[Constant.AW_REMOTE_SCRIPT_PATH]
    self.aw_archive_dir = args[Constant.AW_ARCHIVE_DIR]
    self.bteq_source_target_override = args[Constant.AW_BTEQ_SOURCE_TARGET_OVERRIDE]
    self.metric_override = args[Constant.AW_METRIC_OVERRIDE]
    self.skip_already_parsed = args[Constant.AW_SKIP_ALREADY_PARSED]
    self.look_back_days = args[Constant.AW_LINEAGE_ETL_LOOKBACK_KEY]
    self.last_execution_unix_time = None
    self.get_last_execution_unix_time()

  def get_last_execution_unix_time(self):
    if self.last_execution_unix_time is None:
      try:
        query = """
          SELECT MAX(job_finished_unixtime) as last_time FROM job_execution_data_lineage
          """
        self.aw_cursor.execute(query)
        rows = DbUtil.dict_cursor(self.aw_cursor)
        if rows:
          for row in rows:
            self.last_execution_unix_time = row['last_time']
            break
      except:
        self.logger.error("Get the last execution time from job_execution_data_lineage failed")
        self.last_execution_unix_time = None

    return self.last_execution_unix_time

  def run(self):
    self.logger.info("Begin Appworx Log Parsing")
    try:
      self.process_li_bteq()
      self.process_li_pig()
      self.process_li_hadoop()
      self.process_li_tpt_insert()
      self.process_kafka_sonora_hadoop_get()
      self.process_li_shell_gw()
      self.process_li_getreplacemergegeneral('LI_GETREPLACEMERGEGENERAL')
      self.process_li_getreplacemergegeneral('LINKEDIN_SHELL')
      self.process_li_getreplacemergegeneral('LI_WEBHDFS_GET')
    finally:
      self.aw_cursor.close()
      self.aw_con.close()
    self.logger.info("Finish Appworx Extract")

  def db_lookup(self, dbname, default=None):
    query = \
        """
        SELECT db_id FROM cfg_database WHERE db_code = '%s' or short_connection_string = '%s'
        """
    self.aw_cursor.execute(query % (dbname,dbname))
    rows = DbUtil.dict_cursor(self.aw_cursor)
    for row in rows:
      return row['db_id']

    return 0

  def get_log_file_name(self, module_name, days_offset=1):
    if self.last_execution_unix_time:
      query =  \
        """select je.*, fj.job_type, fl.flow_path,
           CONCAT('%s',CONCAT(CONCAT(DATE_FORMAT(FROM_UNIXTIME(je.start_time), '%%Y%%m%%d'),'/o'),
             CONCAT(je.job_exec_id, '.', LPAD(je.attempt_id, 2, 0), '.gz'))) as gzipped_file_name
           from job_execution je
           JOIN flow_job fj on je.app_id = fj.app_id and je.flow_id = fj.flow_id and fj.is_current = 'Y' and
           je.job_id = fj.job_id
           JOIN flow fl on fj.app_id = fl.app_id and fj.flow_id = fl.flow_id
           WHERE je.app_id = %d
           and je.start_time >= UNIX_TIMESTAMP(DATE_SUB(from_unixtime(%d), INTERVAL 1 day))
           and UPPER(fj.job_type) = '%s'
        """
      self.aw_cursor.execute(query %
                             (self.aw_archive_dir, self.app_id, long(self.last_execution_unix_time), module_name))
    else:
      query = \
        """select je.*, fj.job_type, fl.flow_path,
           CONCAT('%s',CONCAT(CONCAT(DATE_FORMAT(FROM_UNIXTIME(je.start_time), '%%Y%%m%%d'),'/o'),
             CONCAT(je.job_exec_id, '.', LPAD(je.attempt_id, 2, 0), '.gz'))) as gzipped_file_name
           from job_execution je
           JOIN flow_job fj on je.app_id = fj.app_id and je.flow_id = fj.flow_id and fj.is_current = 'Y' and
           je.job_id = fj.job_id
           JOIN flow fl on fj.app_id = fl.app_id and fj.flow_id = fl.flow_id
           WHERE je.app_id = %d  and
           from_unixtime(je.start_time) >= CURRENT_DATE - INTERVAL %d DAY and UPPER(fj.job_type) = '%s'
        """
      self.aw_cursor.execute(query % (self.aw_archive_dir, self.app_id, int(self.look_back_days), module_name))
    job_rows = DbUtil.copy_dict_cursor(self.aw_cursor)

    return job_rows

  def clean_up_staging_lineage_dbs(self, app_id, job_id, job_exec_id, attempt_number):
    clean_source_code_query = \
        """
        DELETE FROM job_attempt_source_code WHERE application_id = %d and job_id = %d and attempt_number = %d
        """
    self.aw_cursor.execute(clean_source_code_query % (int(app_id), int(job_id), int(attempt_number)))

    clean_staging_lineage_query = \
        """
        DELETE FROM stg_job_execution_data_lineage WHERE app_id = %d and job_exec_id = %d
        """
    self.aw_cursor.execute(clean_staging_lineage_query % (int(app_id), int(job_exec_id)))

  def process_li_bteq(self):
    self.logger.info("process li bteq")
    bteq_rows = self.get_log_file_name(module_name='LI_BTEQ')
    kfk_rows = self.get_log_file_name(module_name='KFK_SONORA_STG_TO_FACT')
    rows = bteq_rows + kfk_rows

    parameter_query = \
        """
        SELECT param_value
        FROM   job_parameter
        WHERE  app_id = %d
        AND    job_exec_id = %d
        AND    attempt_number = %d
        ORDER BY param_no
        """
    check_parsed_source_code_query = \
        """
        SELECT application_id, job_id, attempt_number
        FROM   job_attempt_source_code
        WHERE  script_name = '%s'
        AND    script_path = '%s'
        AND    script_md5_sum = CASE WHEN '%s' = 'None' THEN NULL ELSE UNHEX('%s') END
        AND    application_id = %d
        ORDER BY job_id DESC
        LIMIT 1
        """
    check_parsed_lineage_query = \
        """
        SELECT *
        FROM   stg_job_execution_data_lineage
        WHERE  app_id = %d
        AND    job_exec_id = %d
        """
    update_staging_lineage_query = \
        """
        INSERT IGNORE INTO stg_job_execution_data_lineage (
          app_id, flow_exec_id, job_exec_id, flow_path, job_name, job_start_unixtime, job_finished_unixtime,
          db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,
          storage_type, source_target_type, srl_no, source_srl_no, operation, record_count, insert_count,
          created_date, wh_etl_exec_id)
          SELECT %d, %d, %d, '%s', '%s', %d, %d, %d, '%s', '%s',
          CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
          CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
          CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
          '%s', '%s', %d,
          CASE WHEN %d = 0 THEN NULL ELSE %d END,
          '%s', %d, %d, UNIX_TIMESTAMP(now()), %d
        FROM dual
        """
    for row in rows:
      try:
        self.logger.info('Parsing log file: %s' % row['gzipped_file_name'])
        self.aw_cursor.execute(parameter_query %
                             (int(row['app_id']), int(row['job_exec_id']), int(row['attempt_id'])))
        params = DbUtil.copy_dict_cursor(self.aw_cursor)
        key_values = {}
        for param in params:
          ParseUtil.value_from_cmd_line(param['param_value'], key_values)
        analyzed_script = False
        results = AppworxLogParser(log_file_name = row['gzipped_file_name']).parse_log({}, command_type='LI_BTEQ')
        if any(results) == 0:
          self.logger.info('Skipped parsing %s' % row['gzipped_file_name'])
          continue
        self.logger.info(str(results))
        self.logger.info('Completed parsing log file: %s' % row['gzipped_file_name'])

        # Compare md5 string of the file with the one parsed last time
        md5_str = hashlib.md5()
        try:
          md5_str.update(open(self.local_script_path +
                            results['script_path'] + '/' +
                            results['script_name']).read())
        except IOError:
          self.logger.warn("Fails to find script file: %s/%s. Skipping the file" % (results['script_path'], results['script_name']))
          continue

        self.aw_cursor.execute(check_parsed_source_code_query %
                             (results['script_name'],
                              results['script_path'],
                              md5_str,
                              md5_str.hexdigest(),
                              int(row['app_id'])))
        parsed_scripts = DbUtil.copy_dict_cursor(self.aw_cursor)
        if len(parsed_scripts) != 0:
          self.aw_cursor.execute(check_parsed_lineage_query %
                               (int(row['app_id']),
                                int(row['job_exec_id'])))

          parsed_lineage = DbUtil.copy_dict_cursor(self.aw_cursor)
          if len(parsed_lineage) == 0 or self.skip_already_parsed == 'N':
            analyzed_script = False
          else:
            self.logger.debug("%s/%s has already been analyzed. Skipping..." %
                            (results['script_path'], results['script_name']))
            analyzed_script = True
        self.clean_up_staging_lineage_dbs(row['app_id'], row['job_id'], row['job_exec_id'], row['attempt_id'])
        update_source_code_query = \
          """
          INSERT INTO job_attempt_source_code(
            application_id, job_id, attempt_number, script_name, script_path, script_type, created_date, script_md5_sum)
          VALUES( %d, %d, %d, '%s', '%s', '%s', now(), CASE WHEN '%s' = 'None' THEN NULL ELSE UNHEX('%s') END)
          """ % (int(row['app_id']), int(row['job_id']), int(row['attempt_id']),
                 results['script_name'], results['script_path'],
                 'SQL', md5_str, md5_str.hexdigest())

        db_id = 0
        try:
          db_id = self.db_lookup(results['host'])
        except KeyError:
          self.logger.error(sys.exc_info()[0])
        bteq_load_srl = 1
        if not analyzed_script:
          if 'table' in results:
            schema_name = ''
            if 'full_path' in results['table'][0] and results['table'][0]['full_path']:
              index = results['table'][0]['full_path'].index('.')
              if index != -1:
                schema_name = results['table'][0]['full_path'][0:index]
            elif 'schema_name' in results['table'][0] and results['table'][0]['schema_name']:
              schema_name = results['table'][0]['schema_name']
            self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']), 0,
                                  ('/' + schema_name + '/' + results['table'][0]['abstracted_path']) \
                                      if schema_name else results['table'][0]['abstracted_path'], results['table'][0]['full_path'],
                                  None, None, None, None, None, None, results['table'][0]['storage_type'],
                                  results['table'][0]['table_type'], bteq_load_srl, 0, 0,
                                  'Read',
                                  0, 0, int(row['wh_etl_exec_id']) ))
            bteq_load_srl = bteq_load_srl + 1
            schema_name = ''
            if 'full_path' in results['table'][1] and results['table'][1]['full_path']:
              full_table_name = results['table'][1]['full_path']
              index = full_table_name.index('.')
              if index != -1:
                schema_name = full_table_name[0:index]
            elif 'schema_name' in results['table'][1] and results['table'][1]['schema_name']:
              full_table_name = results['table'][1]['schema_name'] + '.' + results['table'][1]['table_name']
              schema_name = results['table'][1]['schema_name']
            else:
              full_table_name = results['table'][1]['table_name']
            self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']), db_id,
                                  ('/' + schema_name + '/' + results['table'][1]['table_name']) \
                                    if schema_name else results['table'][1]['table_name'],
                                  full_table_name, None, None, None, None, None, None, 'Teradata',
                                  results['table'][0]['table_type'], bteq_load_srl, 0, 0,
                                  'Load',
                                  0, 0, int(row['wh_etl_exec_id']) ))
            self.logger.info("Parsing script: %s/%s" % (results['script_path'], results['script_name']))
            self.aw_con.commit()

            entries = BteqLogParser().parse(
              key_values,
              self.local_script_path +
              results['script_path'] + '/' +
              results['script_name'],
              results['script_path'],
              results['script_name'], self.bteq_source_target_override, self.metric_override
            )
            metric_idx = 1
            for srl, e in enumerate(entries):
              schema_name, table_name = ParseUtil.get_db_table_abstraction(e['relation'])
              full_table_name = schema_name + '.' + table_name
              self.aw_cursor.execute(update_staging_lineage_query %
                                   (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                    row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                    db_id, '/' + schema_name + '/' + table_name, full_table_name,
                                    None, None, None, None, None, None, e['storage_type'],
                                    e['table_type'], (bteq_load_srl + srl + 1), 0, 0,
                                    e['operation'],
                                    0, 0, int(row['wh_etl_exec_id']) ))
            self.aw_con.commit()
        else:
          for p in parsed_lineage:
            p['database_id'] = db_id
            self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                  p['database_id'], p['abstracted_object_name'], p['full_object_name'],
                                  p['partition_start'], p['partition_start'], p['partition_end'], p['partition_end'],
                                  p['partition_type'], p['partition_type'], p['storage_type'],
                                  p['source_target_type'], p['srl_no'], 0, 0,
                                  p['operation'],
                                  0, 0, int(row['wh_etl_exec_id']) ))
          self.aw_con.commit()
        self.logger.info('Completed processing metadata for log file: %s' % row['gzipped_file_name'])
      except:
        self.logger.error(str(sys.exc_info()[0]))

  def process_li_getreplacemergegeneral(self, module_name):
    self.logger.info("process %s" % module_name)
    if module_name not in ['LI_GETREPLACEMERGEGENERAL', 'LINKEDIN_SHELL','LI_WEBHDFS_GET']: return
    parameter_query = \
        """
        SELECT GROUP_CONCAT(param_value,'\x01') args
        FROM   job_parameter
        WHERE  app_id = %d
        AND    job_exec_id = %d
        AND    attempt_number = %d
        ORDER BY param_no
        """
    rows = self.get_log_file_name(module_name=module_name)
    for row in rows:
      try:
        self.logger.info('Parsing log file: %s' % row['gzipped_file_name'])
        if module_name == 'LI_GETREPLACEMERGEGENERAL':
          self.aw_cursor.execute(parameter_query %
                                   (int(row['app_id']), int(row['job_exec_id']), int(row['attempt_id'])))
          arg_values = DbUtil.copy_dict_cursor(self.aw_cursor)
          if arg_values and len(arg_values) > 0:
            args = arg_values[0]['args']

        results = AppworxLogParser(log_file_name = row['gzipped_file_name']).parse_log({}, command_type=module_name)
        if any(results) == 0 or not 'cluster' in results:
          self.logger.info('Skipped parsing %s' % row['gzipped_file_name'])
          continue
        self.logger.info(str(results))
        matched_cluster = re.match(r'(.*)_.*', results['cluster'])
        if matched_cluster is not None:
          results['cluster'] = matched_cluster.group(1)
        db_id = int(self.db_lookup(results['cluster']))
        self.logger.info(str(db_id))
        self.clean_up_staging_lineage_dbs(row['app_id'], row['job_id'], row['job_exec_id'], row['attempt_id'])
        update_source_code_query = \
          """
          INSERT INTO job_attempt_source_code(
            application_id, job_id, attempt_number, script_name, script_path, script_type, created_date)
          VALUES( %d, %d, %d, '%s', '%s', '%s', now())
          """ % (int(row['app_id']), int(row['job_id']), int(row['attempt_id']),
                 results['script_name'], results['script_path'],
                 results['script_type'] if module_name == 'LI_WEBHDFS_GET' else 'Shell')
        update_staging_lineage_query = \
          """
          INSERT IGNORE INTO stg_job_execution_data_lineage (
            app_id, flow_exec_id, job_exec_id, flow_path, job_name, job_start_unixtime, job_finished_unixtime,
            db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,
            storage_type, source_target_type, srl_no, source_srl_no, operation, record_count, insert_count,
            created_date, wh_etl_exec_id)
            SELECT %d, %d, %d, '%s', '%s', %d, %d, %d, '%s', '%s',
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            '%s', '%s', %d,
            CASE WHEN %d = 0 THEN NULL ELSE %d END,
            '%s', %d, %d, UNIX_TIMESTAMP(now()), %d
          FROM dual
          """
        self.aw_cursor.execute(update_source_code_query )
        for k, tab in enumerate(results['table']):
          self.aw_cursor.execute(update_staging_lineage_query %
                                (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                 row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                 db_id if tab['table_type'] == 'source' else 0, tab['abstracted_path'], tab['full_path'],
                                 tab['start_partition'], tab['start_partition'],
                                 tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                 tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                 tab['frequency'], tab['frequency'], tab['storage_type'],
                                 tab['table_type'], k + 1, 0, 0,
                                 'Hadoop Get' if tab['table_type'] == 'source' else 'Move',
                                0, 0, int(row['wh_etl_exec_id']) ))
        self.aw_con.commit()
        self.logger.debug('Completed processing metadata for log file: %s' % row['gzipped_file_name'])
      except:
        self.logger.error(str(sys.exc_info()[0]))

  def process_li_shell_gw(self):
    self.logger.info("process process_li_shell_gw")
    parameter_query = \
        """
        SELECT param_value
        FROM   job_parameter
        WHERE  app_id = %d
        AND    job_exec_id = %d
        AND    attempt_number = %d
        ORDER BY param_no
        """

    rows = self.get_log_file_name(module_name='LI_SHELL_GW')
    self.logger.info(str(len(rows)))
    for row in rows:
      self.aw_cursor.execute(parameter_query %
                                         (int(row['app_id']), int(row['job_exec_id']), int(row['attempt_id'])))
      arg_values = DbUtil.copy_dict_cursor(self.aw_cursor)
      if arg_values and len(arg_values) > 0 and 'param_value' in arg_values[0]:
        args = arg_values[0]['param_value'].replace('"','')
        m = re.match("(.*?/trim.sh)\s+\d+\s+\d+", args)
        if m is not None:
          shell_script_name = os.path.basename(m.group(1))
          shell_script_path = os.path.dirname(m.group(1))
          app_path = shell_script_path.replace(self.remote_hadoop_script_dir,'')
          local_shell_script_path = self.local_script_path + self.remote_script_path + app_path
          self.logger.info(local_shell_script_path + '/' + shell_script_name)
          shell_script_content = open(local_shell_script_path + '/' + shell_script_name).read()
          pig_script = re.search(r'pig\s+\-f\s+(.*?)\s+',shell_script_content)
          if pig_script is not None:
            pig_script_name = os.path.basename(pig_script.group(1))
            pig_script_path = os.path.dirname(pig_script.group(1))
            # Compare md5 string of the file with the one parsed last time
            md5_str = hashlib.md5()
            md5_str.update(shell_script_content)

            self.logger.info('Parsing log file: %s' % row['gzipped_file_name'])
            src_tgt_map = PigLogParser(log_file_name=row['gzipped_file_name']).simple_parser()
            if any(src_tgt_map) == 0:
              self.logger.warn('Pig log parser fails to retrieve relevant information from file: %s. Most probably Pig script did not complete successfully' % row['gzipped_file_name'])
              continue
            self.logger.info(str(src_tgt_map))
            db_id = self.db_lookup(src_tgt_map['cluster'])
            self.clean_up_staging_lineage_dbs(row['app_id'], row['job_id'], row['job_exec_id'], row['attempt_id'])
            update_source_code_query = \
                """
                INSERT INTO job_attempt_source_code(
                  application_id, job_id, attempt_number, script_name, script_path, script_type, created_date)
                VALUES( %d, %d, %d, '%s', '%s', '%s', now())
                """ % (int(row['app_id']), int(row['job_id']), int(row['attempt_id']),
                       pig_script_name, pig_script_path, 'Pig')
            self.aw_cursor.execute(update_source_code_query )
            update_staging_lineage_query = \
                """
                INSERT IGNORE INTO stg_job_execution_data_lineage (
                  app_id, flow_exec_id, job_exec_id, flow_path, job_name, job_start_unixtime, job_finished_unixtime,
                  db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,
                  storage_type, source_target_type, srl_no, source_srl_no, operation, record_count, insert_count,
                  created_date, wh_etl_exec_id)
                  SELECT %d, %d, %d, '%s', '%s', %d, %d, %d, '%s', '%s',
                  CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
                  CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
                  CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
                  '%s', '%s', %d,
                  CASE WHEN %d = 0 THEN NULL ELSE %d END,
                  '%s', %d, %d, UNIX_TIMESTAMP(now()), %d
                FROM dual
                """
            srl = 0
            detail_srl = 1
            for k,v in src_tgt_map.items():
              if k in ['cluster'] or k in ['hive-cluster']: continue
              srl = srl + 1
              self.aw_cursor.execute(update_staging_lineage_query %
                                     (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                      row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                      db_id, src_tgt_map[k]['abstracted_hdfs_path'], None,
                                      src_tgt_map[k]['min_start_partition'], src_tgt_map[k]['min_start_partition'],
                                      src_tgt_map[k]['max_end_partition'] if src_tgt_map[k]['table_type'] != 'snapshot' else None,
                                      src_tgt_map[k]['max_end_partition'] if src_tgt_map[k]['table_type'] != 'snapshot' else None,
                                      src_tgt_map[k]['partition_type'], src_tgt_map[k]['partition_type'], 'HDFS',
                                      src_tgt_map[k]['table_type'], int(srl), 0, 0,
                                      'Load' if src_tgt_map[k]['table_type'] == 'source' else 'Store',
                                      0, 0, int(row['wh_etl_exec_id']) ))


              srl = srl + 1
            self.aw_con.commit()
            self.logger.debug('Completed writing metadata for: %s' % row['gzipped_file_name'])
          else:
            self.logger.error("Fails to get Pig script file name used in GW shell script: %s" % m.group(1))

  def process_kafka_sonora_hadoop_get(self):
    self.logger.info("process kafka_sonora_hadoop_get")
    rows = self.get_log_file_name(module_name='KFK_SONORA_HADOOP_GET')
    for row in rows:
      try:
        self.logger.info('Parsing log file: %s' % row['gzipped_file_name'])
        results = AppworxLogParser(log_file_name = row['gzipped_file_name']).parse_log({}, command_type='KFK_SONORA_HADOOP_GET')
        if any(results) == 0:
          self.logger.info('Skipped parsing %s' % row['gzipped_file_name'])
          continue

        self.logger.info(str(results))
        db_id = int(self.db_lookup(results['cluster']))
        self.clean_up_staging_lineage_dbs(row['app_id'], row['job_id'], row['job_exec_id'], row['attempt_id'])
        update_source_code_query = \
          """
          INSERT INTO job_attempt_source_code(
            application_id, job_id, attempt_number, script_name, script_path, script_type, created_date)
          VALUES( %d, %d, %d, '%s', '%s', '%s', now())
          """ % (int(row['app_id']), int(row['job_id']), int(row['attempt_id']),
                   results['script_name'], results['script_path'], 'Shell')
        self.aw_cursor.execute(update_source_code_query )

        update_staging_lineage_query = \
          """
          INSERT IGNORE INTO stg_job_execution_data_lineage (
            app_id, flow_exec_id, job_exec_id, flow_path, job_name, job_start_unixtime, job_finished_unixtime,
            db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,
            storage_type, source_target_type, srl_no, source_srl_no, operation, record_count, insert_count,
            created_date, wh_etl_exec_id)
            SELECT %d, %d, %d, '%s', '%s', %d, %d, %d, '%s', '%s',
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            '%s', '%s', %d,
            CASE WHEN %d = 0 THEN NULL ELSE %d END,
            '%s', %d, %d, UNIX_TIMESTAMP(now()), %d
          FROM dual
          """
        for k, tab in enumerate(results['table']):
          self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                  db_id, tab['abstracted_path'], tab['full_path'],
                                  tab['start_partition'], tab['start_partition'],
                                  tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                  tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                  tab['frequency'], tab['frequency'], tab['storage_type'],
                                  tab['table_type'], k + 1, 0, 0,
                                  'Hadoop Get' if tab['table_type'] == 'source' else 'Write',
                                  0, 0, int(row['wh_etl_exec_id']) ))
        self.aw_con.commit()
        self.logger.info('Completed processing hadoop for log file: %s' % row['gzipped_file_name'])
      except:
        self.logger.error(str(sys.exc_info()[0]))

  def process_li_tpt_insert(self):
    self.logger.info("process li_tpt_insert")
    rows = self.get_log_file_name(module_name='LI_TPT_INSERT')
    for row in rows:
      try:
        self.logger.info('Parsing log file: %s' % row['gzipped_file_name'])
        results = AppworxLogParser(log_file_name = row['gzipped_file_name']).parse_log({}, command_type='LI_TPT_INSERT')
        if any(results) == 0:
          self.logger.info('Skipped parsing %s' % row['gzipped_file_name'])
          continue

        self.logger.info(str(results))
        db_id = int(self.db_lookup(results['table'][1]['host']))
        self.clean_up_staging_lineage_dbs(row['app_id'], row['job_id'], row['job_exec_id'], row['attempt_id'])
        update_source_code_query = \
            """
            INSERT INTO job_attempt_source_code(
              application_id, job_id, attempt_number, script_name, script_path, script_type, created_date)
            VALUES( %d, %d, %d, '%s', '%s', '%s', now())
            """ % (int(row['app_id']), int(row['job_id']), int(row['attempt_id']),
                 results['script_name'], results['script_path'], 'TPT')
        self.aw_cursor.execute(update_source_code_query )

        update_staging_lineage_query = \
            """
            INSERT IGNORE INTO stg_job_execution_data_lineage (
              app_id, flow_exec_id, job_exec_id, flow_path, job_name, job_start_unixtime, job_finished_unixtime,
              db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,
              storage_type, source_target_type, srl_no, source_srl_no, operation, record_count, insert_count,
              created_date, wh_etl_exec_id)
              SELECT %d, %d, %d, '%s', '%s', %d, %d, %d, '%s', '%s',
              CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
              CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
              CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
              '%s', '%s', %d,
              CASE WHEN %d = 0 THEN NULL ELSE %d END,
              '%s', %d, %d, UNIX_TIMESTAMP(now()), %d
            FROM dual
            """
        tab = results['table'][0]
        self.aw_cursor.execute(update_staging_lineage_query %
                               (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                0, tab['abstracted_path'], tab['full_path'],
                                tab['start_partition'], tab['start_partition'],
                                tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                tab['frequency'], tab['frequency'], tab['storage_type'],
                                tab['table_type'], 1, 0, 0, 'Read',
                                0, 0, int(row['wh_etl_exec_id']) ))
        tab = results['table'][1]  # position 1 is target table
        full_table_name = tab['schema'] + '.' + tab['table_name']
        self.aw_cursor.execute(update_staging_lineage_query %
                               (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                db_id, tab['table_name'], full_table_name,
                                None, None,
                                None, None,
                                None, None, tab['storage_type'],
                                tab['table_type'], 2, 0, 0, 'Write',
                                0, int(tab['insert_count']), int(row['wh_etl_exec_id']) ))
        self.aw_con.commit()
        self.logger.info('Completed processing li_tpt_insert for log file: %s' % row['gzipped_file_name'])
      except:
        self.logger.error(str(sys.exc_info()[0]))


  def process_li_hadoop(self):
    self.logger.info("process li_pig")
    rows = self.get_log_file_name(module_name='LI_HADOOP')
    for row in rows:
      try:
        self.logger.info('Parsing log file: %s' % row['gzipped_file_name'])
        results = AppworxLogParser(log_file_name = row['gzipped_file_name']).parse_log({}, command_type='LI_HADOOP')
        if any(results) == 0:
          self.logger.info('Skipped parsing %s' % row['gzipped_file_name'])
          continue

        self.logger.info(str(results))
        if 'ref_mr_job_ids' in results and 'script_type' in results and results['script_type'] != 'Lassen':
          self.logger.error("MR JOBS:")
          self.logger.error(str(results['ref_mr_job_ids']))
        db_id = int(self.db_lookup(results['cluster']))

        self.clean_up_staging_lineage_dbs(row['app_id'], row['job_id'], row['job_exec_id'], row['attempt_id'])
        update_source_code_query = \
          """
          INSERT INTO job_attempt_source_code(
            application_id, job_id, attempt_number, script_name, script_path, script_type, created_date)
          VALUES( %d, %d, %d, '%s', '%s', '%s', now())
          """ % (int(row['app_id']), int(row['job_id']), int(row['attempt_id']),
                 results['script_name'], results['script_path'], results['script_type'])
        self.aw_cursor.execute(update_source_code_query )
        self.aw_con.commit()

        update_staging_lineage_query = \
          """
          INSERT IGNORE INTO stg_job_execution_data_lineage (
            app_id, flow_exec_id, job_exec_id, flow_path, job_name, job_start_unixtime, job_finished_unixtime,
            db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,
            storage_type, source_target_type, srl_no, source_srl_no, operation, record_count,
            created_date, wh_etl_exec_id)
            SELECT %d, %d, %d, '%s', '%s', %d, %d, %d, '%s', '%s',
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            '%s', '%s', %d,
            CASE WHEN %d = 0 THEN NULL ELSE %d END,
            '%s', %d, UNIX_TIMESTAMP(now()), %d
          FROM dual
          """

        if results['script_type'] == 'Lassen':
          srl = 1
          for index, tab in enumerate(results['table']):
            source_database_id = int(self.db_lookup(tab['source_database']))

            full_table_name = \
              tab['source_schema'] + \
              ('.' if tab['source_schema'] is not None else '') + \
              tab['source_table_name']
            self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                  source_database_id, tab['source_table_name'], full_table_name,
                                  None, None, None, None, None, None, 'Teradata',
                                  'source', int(srl), 0, 0, 'JDBC Read',
                                  0, int(row['wh_etl_exec_id']) ))
            source_srl_no = srl
            srl = srl + 1
            self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                  db_id, tab['abstracted_path'], tab['full_path'],
                                  tab['start_partition'], tab['start_partition'],
                                  tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                  tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                  tab['frequency'], tab['frequency'],
                                  'HDFS',
                                  'target', int(srl), int(source_srl_no), int(source_srl_no), 'JDBC Write',
                                  int(tab['record_count']), int(row['wh_etl_exec_id']) ))

            srl = srl + 1
        elif results['script_type'] == 'CMD':
          db_id = self.db_lookup(results['cluster'])

          for index, tab in enumerate(results['table']):
            self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                  db_id, tab['abstracted_path'], tab['full_path'],
                                  tab['start_partition'], tab['start_partition'],
                                  tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                  tab['end_partition'] if tab['frequency'] != 'snapshot' else None,
                                  tab['frequency'], tab['frequency'],
                                  tab['storage_type'],
                                  'source' if index == 0 else 'target', index + 1, 0, 'Read' if index == 0 else 'Write',
                                  str(tab['record_count']), int(row['wh_etl_exec_id']) ))
        self.aw_con.commit()
        self.logger.info('Completed processing hadoop for log file: %s' % row['gzipped_file_name'])
      except:
        self.logger.error(str(sys.exc_info()[0]))

  def process_li_pig(self):
    self.logger.info("process li_pig")
    rows = self.get_log_file_name(module_name='LI_PIG_JOB')
    results = {}
    for row in rows:
      try:
        self.logger.info(row['gzipped_file_name'])
        results = AppworxLogParser(log_file_name = row['gzipped_file_name']).parse_log({}, command_type='LI_PIG')
        if any(results) == 0:
          self.logger.warn("Log file: %s could not be parsed. Skipping the file" % row['gzipped_file_name'])
          continue
        self.logger.info(str(results))
        src_tgt_map = PigLogParser(log_file_name=row['gzipped_file_name']).simple_parser()
        if any(src_tgt_map) == 0: # check for nonempty dictionary
            self.logger.warn('Pig log inside %s could not be parsed.Skipping the file' % row['gzipped_file_name'])
            continue
        db_id = int(self.db_lookup(src_tgt_map['cluster']))
        if db_id == 0:
          self.logger.error("Fails to get lookup information for database: %s" % src_tgt_map['cluster'])
          return

        hive_db_id = int(self.db_lookup(src_tgt_map['hive-cluster']))
        if hive_db_id == 0:
          self.logger.error("Fails to get lookup information for database: %s" % (src_tgt_map['hive-cluster']))
          hive_db_id = db_id

        clean_source_code_query = \
          """
          DELETE FROM job_attempt_source_code WHERE application_id = %d and job_id = %d and attempt_number = %d
          """
        self.clean_up_staging_lineage_dbs(row['app_id'], row['job_id'], row['job_exec_id'], row['attempt_id'])

        update_source_code_query = \
          """
          INSERT INTO job_attempt_source_code(
            application_id, job_id, attempt_number, script_name, script_path, script_type, created_date)
          VALUES( %d, %d, %d, '%s', '%s', 'Pig', now())
          """ % (int(row['app_id']), int(row['job_id']), int(row['attempt_id']),  results['script_name'], results['script_path'])

        self.aw_cursor.execute(update_source_code_query )
        self.aw_con.commit()

        update_staging_lineage_query = \
          """
          INSERT IGNORE INTO stg_job_execution_data_lineage(
            app_id, flow_exec_id, job_exec_id, flow_path, job_name, job_start_unixtime, job_finished_unixtime,
            db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,
            storage_type, source_target_type, srl_no, operation, record_count,
            created_date, wh_etl_exec_id
          )
          SELECT %d, %d, %d, '%s', '%s', %d, %d, %d, '%s', '%s',
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            CASE WHEN '%s' = 'None' THEN NULL ELSE '%s' END,
            '%s', '%s', %d, '%s', %d, UNIX_TIMESTAMP(now()), %d
          FROM dual
          """
        srl = 0
        detail_srl = 1
        for k,v in src_tgt_map.items():
          if k in ['cluster'] or k in ['hive-cluster']: continue
          srl = srl + 1
          operation = 'Store'
          if src_tgt_map[k]['table_type'] == 'source':
              operation = 'Load'
          partition_end = None
          if src_tgt_map[k]['table_type'] != 'snapshot' and 'max_end_partition' in src_tgt_map[k]:
              partition_end = src_tgt_map[k]['max_end_partition']
          current_db_id = db_id
          if src_tgt_map[k]['storage_type'] == 'HIVE':
            current_db_id = hive_db_id
          self.aw_cursor.execute(update_staging_lineage_query %
                                 (int(row['app_id']), int(row['flow_exec_id']), int(row['job_exec_id']),
                                  row['flow_path'], row['job_name'], int(row['start_time']), int(row['end_time']),
                                  current_db_id, src_tgt_map[k]['abstracted_hdfs_path'], src_tgt_map[k]['hdfs_path'][0],
                                  src_tgt_map[k]['min_start_partition'],src_tgt_map[k]['min_start_partition'],
                                  partition_end, partition_end, src_tgt_map[k]['partition_type'],
                                  src_tgt_map[k]['partition_type'], src_tgt_map[k]['storage_type'],
                                  src_tgt_map[k]['table_type'], int(srl), operation,
                                  int(src_tgt_map[k]['record_count']), int(row['wh_etl_exec_id']) ))
        self.aw_con.commit()

        self.logger.info('Completed parsing Pig log inside %s for script: %s/%s' %
                          (row['gzipped_file_name'],results['script_path'],results['script_name']))
      except:
        self.logger.error(str(sys.exc_info()[0]))

if __name__ == "__main__":
  props = sys.argv[1]
  aw = AppworxLineageExtract(props)
  aw.run()
