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

from wherehows.common.writers import FileWriter
from wherehows.common.schemas import AppworxFlowRecord
from wherehows.common.schemas import AppworxJobRecord
from wherehows.common.schemas import AppworxFlowDagRecord
from wherehows.common.schemas import AppworxFlowExecRecord
from wherehows.common.schemas import AppworxJobExecRecord
from wherehows.common.schemas import AppworxFlowScheduleRecord
from wherehows.common.schemas import AppworxFlowOwnerRecord
from wherehows.common.enums import AzkabanPermission
from wherehows.common import Constant
from wherehows.common.enums import SchedulerType
from com.ziclix.python.sql import zxJDBC
import os
import DbUtil
import sys
import gzip
import StringIO
import json
import datetime
import time
import re
from org.slf4j import LoggerFactory

class AppworxExtract:

  _period_unit_table = {'d': 'DAY',
                        'M': 'MONTH',
                        'h': 'HOUR',
                        'm': 'MINUTE',
                        'w': 'WEEK'}

  def get_connection(self, host, port, sid, username, password, driver):
      jdbc_url = "jdbc:oracle:thin:@%(host)s:%(port)s:%(sid)s" % locals()
      return zxJDBC.connect(jdbc_url, username, password, driver)

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.aw_con = self.get_connection(args[Constant.AW_DB_URL_KEY],
                                    args[Constant.AW_DB_PORT_KEY],
                                    args[Constant.AW_DB_NAME_KEY],
                                    args[Constant.AW_DB_USERNAME_KEY],
                                    args[Constant.AW_DB_PASSWORD_KEY],
                                    args[Constant.AW_DB_DRIVER_KEY])
    self.aw_cursor = self.aw_con.cursor()
    self.lookback_period = args[Constant.AW_EXEC_ETL_LOOKBACK_KEY]
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(SchedulerType.APPWORX) + "/" + str(self.app_id)
    self.last_execution_unix_time = None
    self.get_last_execution_unix_time()

    if not os.path.exists(self.metadata_folder):
      try:
        os.makedirs(self.metadata_folder)
      except Exception as e:
        self.logger.error(e)

  def get_last_execution_unix_time(self):
    if self.last_execution_unix_time is None:
      try:
        query = """
            SELECT MAX(end_time) as last_time FROM job_execution where app_id = %d
            """
        self.wh_cursor.execute(query % self.app_id)
        rows = DbUtil.dict_cursor(self.wh_cursor)
        if rows:
          for row in rows:
            self.last_execution_unix_time = row['last_time']
            break;
      except:
        self.logger.error("Get the last execution time from job_execution failed")
        self.last_execution_unix_time = None

    return self.last_execution_unix_time

  def run(self):
    self.logger.info("Begin Appworx Extract")
    try:
      self.collect_flow_jobs(self.metadata_folder + "/flow.csv", self.metadata_folder + "/job.csv", self.metadata_folder + "/dag.csv")
      self.collect_flow_owners(self.metadata_folder + "/owner.csv")
      self.collect_flow_execs(self.metadata_folder + "/flow_exec.csv", self.metadata_folder + "/job_exec.csv", self.lookback_period)
      self.collect_flow_schedules(self.metadata_folder + "/schedule.csv")
      self.logger.info("Finish Appworx Extract")
    finally:
      self.aw_cursor.close()
      self.wh_cursor.close()
      self.aw_con.close()
      self.wh_con.close()

  def collect_flow_jobs(self, flow_file, job_file, dag_file):
    self.logger.info("collect flow&jobs")
    timezone = "ALTER SESSION SET TIME_ZONE = 'US/Pacific'"
    self.aw_cursor.execute(timezone)
    schema = "ALTER SESSION SET CURRENT_SCHEMA=APPWORX"
    self.aw_cursor.execute(schema)
    if self.last_execution_unix_time:
      query = \
        """SELECT J.*, R.RUNS
           FROM SO_JOB_TABLE J JOIN (
           SELECT SO_JOB_SEQ, COUNT(*) as RUNS
           FROM SO_JOB_HISTORY
           WHERE cast((FROM_TZ(CAST(SO_JOB_FINISHED as timestamp), 'US/Pacific') at time zone 'GMT') as date) >=
           (TO_DATE('1970-01-01','YYYY-MM-DD') + (%d - 3600) / 86400)
           GROUP BY SO_JOB_SEQ
           ) R ON J.SO_JOB_SEQ = R.SO_JOB_SEQ
           WHERE SO_COMMAND_TYPE = 'CHAIN'
        """ % long(self.last_execution_unix_time)
    else:
      query = \
        """SELECT J.*, R.RUNS
           FROM SO_JOB_TABLE J JOIN (
           SELECT SO_JOB_SEQ, COUNT(*) as RUNS
           FROM SO_JOB_HISTORY
           WHERE SO_JOB_FINISHED >= SYSDATE - %d
           GROUP BY SO_JOB_SEQ
           ) R ON J.SO_JOB_SEQ = R.SO_JOB_SEQ
           WHERE SO_COMMAND_TYPE = 'CHAIN'
        """ % int(self.lookback_period)
    job_query = \
        """SELECT d.SO_TASK_NAME, d.SO_CHAIN_ORDER, d.SO_PREDECESSORS as PREDECESSORS, d.SO_DET_SEQ as JOB_ID,
            t.* FROM SO_CHAIN_DETAIL d
            JOIN SO_JOB_TABLE t ON d.SO_JOB_SEQ = t.SO_JOB_SEQ
            WHERE d.SO_CHAIN_SEQ = %d ORDER BY d.SO_CHAIN_ORDER
        """
    self.aw_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.aw_cursor)
    flow_writer = FileWriter(flow_file)
    job_writer = FileWriter(job_file)
    dag_writer = FileWriter(dag_file)
    row_count = 0

    for row in rows:

      flow_path = row['SO_APPLICATION'] + ":" + row['SO_MODULE']

      flow_record = AppworxFlowRecord(self.app_id,
                                      long(row['SO_JOB_SEQ']),
                                      row['SO_MODULE'],
                                      row['SO_APPLICATION'],
                                      flow_path,
                                      0,
                                      0,
                                      0,
                                      'Y',
                                      self.wh_exec_id)
      flow_writer.append(flow_record)
      new_appworx_cursor = self.aw_con.cursor()
      new_appworx_cursor.execute(job_query % row['SO_JOB_SEQ'])
      job_rows = DbUtil.dict_cursor(new_appworx_cursor)
      for job in job_rows:
        job_record = AppworxJobRecord(self.app_id,
                                      long(row['SO_JOB_SEQ']),
                                      flow_path,
                                      0,
                                      long(job['JOB_ID']),
                                      job['SO_TASK_NAME'],
                                      flow_path + '/' + job['SO_TASK_NAME'],
                                      job['SO_MODULE'],
                                      'Y',
                                      self.wh_exec_id)
        command_type = job['SO_COMMAND_TYPE']
        if command_type and command_type == 'CHAIN':
          job_record.setRefFlowPath(job['SO_APPLICATION'] + ":" + job['SO_MODULE'])
          job_record.setJobType('CHAIN')

        job_writer.append(job_record)

        predecessors_str = job['PREDECESSORS']
        if predecessors_str:
          predecessors = re.findall(r"\&\/(.+?)\s\=\sS", predecessors_str)
          if predecessors:
            for predecessor in predecessors:
              dag_edge = AppworxFlowDagRecord(self.app_id,
                                             long(row['SO_JOB_SEQ']),
                                             flow_path,
                                             0,
                                             flow_path + '/' + predecessor,
                                             flow_path + '/' + job['SO_TASK_NAME'],
                                             self.wh_exec_id)
              dag_writer.append(dag_edge)
      row_count += 1

      if row_count % 1000 == 0:
        flow_writer.flush()
        job_writer.flush()
        dag_writer.flush()

    flow_writer.close()
    job_writer.close()
    dag_writer.close()

  def collect_flow_execs(self, flow_exec_file, job_exec_file, look_back_period):
    self.logger.info("collect flow&job executions")
    flow_exec_writer = FileWriter(flow_exec_file)
    job_exec_writer = FileWriter(job_exec_file)
    timezone = "ALTER SESSION SET TIME_ZONE = 'US/Pacific'"
    self.aw_cursor.execute(timezone)
    schema = "ALTER SESSION SET CURRENT_SCHEMA=APPWORX"
    self.aw_cursor.execute(schema)
    flow_id_list = []
    if self.last_execution_unix_time:
      flow_cmd = \
        """SELECT J.SO_JOB_SEQ, J.SO_MODULE, J.SO_APPLICATION, H.SO_STATUS_NAME, H.SO_JOBID, H.SO_CHAIN_ID,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_STARTED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_STARTED,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_FINISHED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_FINISHED,
           U.SO_USER_NAME FROM SO_JOB_TABLE J
           JOIN SO_JOB_HISTORY H ON J.SO_JOB_SEQ = H.SO_JOB_SEQ
           LEFT JOIN SO_USER_TABLE U ON H.SO_USER_SEQ = U.SO_USER_SEQ
           WHERE H.SO_JOB_FINISHED >= (TO_DATE('1970-01-01','YYYY-MM-DD') + (%d -3600)/ 86400) and
           J.SO_COMMAND_TYPE = 'CHAIN' """ % self.last_execution_unix_time
    else:
      flow_cmd = \
        """SELECT J.SO_JOB_SEQ, J.SO_MODULE, J.SO_APPLICATION, H.SO_STATUS_NAME, H.SO_JOBID, H.SO_CHAIN_ID,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_STARTED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_STARTED,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_FINISHED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_FINISHED,
           U.SO_USER_NAME FROM SO_JOB_TABLE J
           JOIN SO_JOB_HISTORY H ON J.SO_JOB_SEQ = H.SO_JOB_SEQ
           LEFT JOIN SO_USER_TABLE U ON H.SO_USER_SEQ = U.SO_USER_SEQ
           WHERE H.SO_JOB_FINISHED >= SYSDATE - %d and
           J.SO_COMMAND_TYPE = 'CHAIN' """ % int(self.lookback_period)

    if self.last_execution_unix_time:
      job_cmd = \
        """SELECT D.SO_TASK_NAME, U.SO_USER_NAME, H.SO_STATUS_NAME, H.SO_JOBID, D.SO_DET_SEQ as JOB_ID,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_STARTED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_STARTED,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_FINISHED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_FINISHED
           FROM SO_JOB_HISTORY H
           JOIN SO_CHAIN_DETAIL D ON D.SO_CHAIN_SEQ = H.SO_CHAIN_SEQ AND D.SO_DET_SEQ = H.SO_DET_SEQ
           LEFT JOIN SO_USER_TABLE U ON H.SO_USER_SEQ = U.SO_USER_SEQ
           WHERE H.SO_JOB_FINISHED >= (TO_DATE('1970-01-01','YYYY-MM-DD') + (%d - 3600)/ 86400) and
           H.SO_PARENTS_JOBID = %d and H.SO_CHAIN_ID = %d"""
    else:
      job_cmd = \
        """SELECT D.SO_TASK_NAME, U.SO_USER_NAME, H.SO_STATUS_NAME, H.SO_JOBID, D.SO_DET_SEQ as JOB_ID,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_STARTED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_STARTED,
           ROUND((cast((FROM_TZ(CAST(H.SO_JOB_FINISHED as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as JOB_FINISHED
           FROM SO_JOB_HISTORY H
           JOIN SO_CHAIN_DETAIL D ON D.SO_CHAIN_SEQ = H.SO_CHAIN_SEQ AND D.SO_DET_SEQ = H.SO_DET_SEQ
           LEFT JOIN SO_USER_TABLE U ON H.SO_USER_SEQ = U.SO_USER_SEQ
           WHERE H.SO_JOB_FINISHED >= SYSDATE - %d and
           H.SO_PARENTS_JOBID = %d and H.SO_CHAIN_ID = %d"""

    self.aw_cursor.execute(flow_cmd)

    rows = DbUtil.dict_cursor(self.aw_cursor)
    row_count = 0
    for row in rows:
      flow_path = row['SO_APPLICATION'] + ":" + row['SO_MODULE']
      so_flow_id = row['SO_JOBID']
      flow_attempt = 0
      flow_exec_id = 0
      try:
        flow_attempt = int(float(str(so_flow_id - int(so_flow_id))[1:])*100)
        flow_exec_id = int(so_flow_id)
      except Exception as e:
        self.logger.error(e)

      flow_exec_record = AppworxFlowExecRecord(self.app_id,
                                               long(row['SO_JOB_SEQ']),
                                               row['SO_MODULE'],
                                               flow_path,
                                               0,
                                               flow_exec_id,
                                               row['SO_STATUS_NAME'],
                                               flow_attempt,
                                               row['SO_USER_NAME'],
                                               long(row['JOB_STARTED']),
                                               long(row['JOB_FINISHED']),
                                               self.wh_exec_id)
      flow_exec_writer.append(flow_exec_record)

      new_appworx_cursor = self.aw_con.cursor()
      if self.last_execution_unix_time:
        new_appworx_cursor.execute(job_cmd % (self.last_execution_unix_time, flow_exec_id, long(row['SO_CHAIN_ID'])))
      else:
        new_appworx_cursor.execute(job_cmd % (int(self.lookback_period), flow_exec_id, long(row['SO_CHAIN_ID'])))
      job_rows = DbUtil.dict_cursor(new_appworx_cursor)

      for job in job_rows:
        so_job_id = job['SO_JOBID']
        job_attempt = 0
        job_exec_id = 0
        try:
          job_attempt = int(float(str(so_job_id - int(so_job_id))[1:])*100)
          job_exec_id = int(so_job_id)
        except Exception as e:
          self.logger.error(e)

        job_exec_record = AppworxJobExecRecord(self.app_id,
                                               long(row['SO_JOB_SEQ']),
                                               flow_path,
                                               0,
                                               flow_exec_id,
                                               long(job['JOB_ID']),
                                               job['SO_TASK_NAME'],
                                               flow_path + "/" + job['SO_TASK_NAME'],
                                               job_exec_id,
                                               job['SO_STATUS_NAME'],
                                               job_attempt,
                                               long(job['JOB_STARTED']),
                                               long(job['JOB_FINISHED']),
                                               self.wh_exec_id)

        job_exec_writer.append(job_exec_record)
        row_count += 1
      if row_count % 10000 == 0:
        flow_exec_writer.flush()
        job_exec_writer.flush()

    flow_exec_writer.close()
    job_exec_writer.close()

  def collect_flow_schedules(self, schedule_file):
    # load flow scheduling info from table triggers
    self.logger.info("collect flow schedule")
    timezone = "ALTER SESSION SET TIME_ZONE = 'US/Pacific'"
    self.aw_cursor.execute(timezone)
    schema = "ALTER SESSION SET CURRENT_SCHEMA=APPWORX"
    self.aw_cursor.execute(schema)
    schedule_writer = FileWriter(schedule_file)
    query = \
        """SELECT J.SO_APPLICATION, J.SO_MODULE, S.AW_SCH_NAME, S.AW_SCH_INTERVAL, S.AW_ACTIVE,
           ROUND((cast((FROM_TZ(CAST(S.AW_SCH_START as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as EFFECT_STARTED,
           ROUND((cast((FROM_TZ(CAST(S.AW_SCH_END as timestamp), 'US/Pacific') at time zone 'GMT') as date) -
           to_date('01-JAN-1970','DD-MON-YYYY'))* (86400)) as EFFECT_END
           FROM SO_JOB_TABLE J
           JOIN AW_MODULE_SCHED S ON J.SO_JOB_SEQ = S.AW_JOB_SEQ
           WHERE J.SO_COMMAND_TYPE = 'CHAIN' AND S.AW_ACTIVE = 'Y' """
    self.aw_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.aw_cursor)
    for row in rows:
      schedule_record = AppworxFlowScheduleRecord(self.app_id,
                                                  row['SO_APPLICATION'] + ":" + row['SO_MODULE'],
                                                  row['AW_SCH_NAME'],
                                                  int(row['AW_SCH_INTERVAL']),
                                                  long(row['EFFECT_STARTED']),
                                                  long(row['EFFECT_END']),
                                                  '0',
                                                  self.wh_exec_id
                                                  )
      schedule_writer.append(schedule_record)
    schedule_writer.close()

  def collect_flow_owners(self, owner_file):
    self.logger.info("collect owner&permissions")
    timezone = "ALTER SESSION SET TIME_ZONE = 'US/Pacific'"
    self.aw_cursor.execute(timezone)
    schema = "ALTER SESSION SET CURRENT_SCHEMA=APPWORX"
    self.aw_cursor.execute(schema)
    user_writer = FileWriter(owner_file)
    query = \
        """SELECT DISTINCT J.SO_JOB_SEQ, J.SO_MODULE, J.SO_APPLICATION, U.SO_USER_NAME FROM SO_JOB_TABLE J
             JOIN SO_JOB_HISTORY H ON J.SO_JOB_SEQ = H.SO_JOB_SEQ
             JOIN SO_USER_TABLE U ON H.SO_USER_SEQ = U.SO_USER_SEQ
             WHERE J.SO_COMMAND_TYPE = 'CHAIN' """
    self.aw_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.aw_cursor)

    for row in rows:
      record = AppworxFlowOwnerRecord(self.app_id,
                                      row['SO_APPLICATION'] + ':' + row["SO_MODULE"],
                                      row["SO_USER_NAME"],
                                      'EXECUTE',
                                      'GROUP',
                                      self.wh_exec_id)
      user_writer.append(record)
    user_writer.close()

if __name__ == "__main__":
  props = sys.argv[1]
  aw = AppworxExtract(props)
  aw.run()
