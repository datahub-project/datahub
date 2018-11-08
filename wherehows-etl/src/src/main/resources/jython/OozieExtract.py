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
from wherehows.common import Constant
from wherehows.common.schemas import OozieFlowRecord
from wherehows.common.schemas import OozieJobRecord
from wherehows.common.schemas import OozieFlowOwnerRecord
from wherehows.common.schemas import OozieFlowExecRecord
from wherehows.common.schemas import OozieJobExecRecord
from wherehows.common.schemas import OozieFlowScheduleRecord
from wherehows.common.schemas import OozieFlowDagRecord
from wherehows.common.enums import SchedulerType
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
import os
import DbUtil
import sys


class OozieExtract:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.JOB_REF_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.oz_con = zxJDBC.connect(args[Constant.OZ_DB_URL_KEY],
                                 args[Constant.OZ_DB_USERNAME_KEY],
                                 args[Constant.OZ_DB_PASSWORD_KEY],
                                 args[Constant.OZ_DB_DRIVER_KEY])
    self.oz_cursor = self.oz_con.cursor()
    self.lookback_period = args[Constant.OZ_EXEC_ETL_LOOKBACK_MINS_KEY]
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(SchedulerType.OOZIE) + "/" + str(self.app_id)
    self.oz_version = 4.0
    if not os.path.exists(self.metadata_folder):
      try:
        os.makedirs(self.metadata_folder)
      except Exception as e:
        self.logger.error(e)

    self.get_oozie_version()

  def get_oozie_version(self):
    query = "select data from OOZIE_SYS where name = 'oozie.version'"
    self.oz_cursor.execute(query)
    self.oz_version = self.oz_cursor.fetchone()
    self.logger.info("Oozie version: ", self.oz_version[0])

  def run(self):
    try:
      self.collect_flow_jobs(self.metadata_folder + "/flow.csv",
                             self.metadata_folder + "/job.csv",
                             self.metadata_folder + "/dag.csv")
      self.collect_flow_owners(self.metadata_folder + "/owner.csv")
      self.collect_flow_schedules(self.metadata_folder + "/schedule.csv")
      self.collect_flow_execs(self.metadata_folder + "/flow_exec.csv", self.lookback_period)
      self.collect_job_execs(self.metadata_folder + "/job_exec.csv", self.lookback_period)
    finally:
      self.oz_cursor.close()
      self.oz_con.close()

  def collect_flow_jobs(self, flow_file, job_file, dag_file):
    self.logger.info("collect flow&jobs")
    flow_writer = FileWriter(flow_file)
    job_writer = FileWriter(job_file)
    dag_writer = FileWriter(dag_file)
    query = """
            SELECT a.*, b.created_time FROM
              (SELECT w.app_name, w.app_path, max(w.id) as source_version, max(unix_timestamp(w.last_modified_time)) as last_modified_time
              from WF_JOBS w LEFT JOIN WF_JOBS s
              ON w.app_path = s.app_path AND w.created_time < s.created_time
              WHERE s.created_time IS NULL GROUP BY w.app_name, w.app_path) a
              JOIN
              (SELECT app_path, min(unix_timestamp(created_time)) as created_time FROM WF_JOBS GROUP BY app_path) b
              ON a.app_path = b.app_path
            """
    self.oz_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.oz_cursor)

    for row in rows:
      flow_record = OozieFlowRecord(self.app_id,
                                    row['app_name'],
                                    row['app_path'],
                                    0,
                                    row['source_version'],
                                    row['created_time'],
                                    row['last_modified_time'],
                                    self.wh_exec_id)
      flow_writer.append(flow_record)
      query = """
              select name, type, transition from WF_ACTIONS
              where wf_id = '{source_version}'
              """.format(source_version=row['source_version'])
      new_oz_cursor = self.oz_con.cursor()
      new_oz_cursor.execute(query)
      nodes = DbUtil.dict_cursor(new_oz_cursor)

      for node in nodes:
        job_record = OozieJobRecord(self.app_id,
                                    row['app_path'],
                                    row['source_version'],
                                    node['name'],
                                    row['app_path'] + "/" + node['name'],
                                    node['type'],
                                    self.wh_exec_id)
        job_writer.append(job_record)

        if node['transition'] != "*" and node['transition'] is not None:
          dag_edge = OozieFlowDagRecord(self.app_id,
                                        row['app_path'],
                                        row['source_version'],
                                        row['app_path'] + "/" + node['name'],
                                        row['app_path'] + "/" + node['transition'],
                                        self.wh_exec_id)
          dag_writer.append(dag_edge)
      new_oz_cursor.close()

    dag_writer.close()
    job_writer.close()
    flow_writer.close()

  def collect_flow_owners(self, owner_file):
    self.logger.info("collect owners")
    owner_writer = FileWriter(owner_file)
    query = "SELECT DISTINCT app_name, app_path, user_name from WF_JOBS"
    self.oz_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.oz_cursor)

    for row in rows:
      owner_record = OozieFlowOwnerRecord(self.app_id,
                                          row['app_path'],
                                          row['user_name'],
                                          self.wh_exec_id)
      owner_writer.append(owner_record)
    owner_writer.close()

  def collect_flow_schedules(self, schedule_file):
    self.logger.info("collect flow schedule")
    schedule_writer = FileWriter(schedule_file)
    query = """
            SELECT DISTINCT cj.id as ref_id, cj.frequency, cj.time_unit,
            unix_timestamp(cj.start_time) as start_time, unix_timestamp(cj.end_time) as end_time,
            wj.app_path
            FROM COORD_JOBS cj JOIN COORD_ACTIONS ca ON ca.job_id = cj.id JOIN WF_JOBS wj ON ca.external_id = wj.id
            WHERE cj.status = 'RUNNING'
            """
    self.oz_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.oz_cursor)

    for row in rows:
      schedule_record = OozieFlowScheduleRecord(self.app_id,
                                                row['app_path'],
                                                row['time_unit'],
                                                str(row['frequency']),
                                                None,
                                                row['start_time'],
                                                row['end_time'],
                                                row['ref_id'],
                                                self.wh_exec_id)
      schedule_writer.append(schedule_record)

    schedule_writer.close()

  def collect_flow_execs(self, flow_exec_file, lookback_period):
    self.logger.info("collect flow execs")
    flow_exec_writer = FileWriter(flow_exec_file)
    query = "select id, app_name, app_path, unix_timestamp(start_time) as start_time, unix_timestamp(end_time) as end_time, run, status, user_name from WF_JOBS where end_time > now() - INTERVAL %d MINUTE" % (int(lookback_period))
    self.oz_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.oz_cursor)

    for row in rows:
      flow_exec_record = OozieFlowExecRecord(self.app_id,
                                             row['app_name'],
                                             row['app_path'],
                                             row['id'],
                                             row['id'],
                                             row['status'],
                                             row['run'],
                                             row['user_name'],
                                             row['start_time'],
                                             row['end_time'],
                                             self.wh_exec_id)
      flow_exec_writer.append(flow_exec_record)

    flow_exec_writer.close()

  def collect_job_execs(self, job_exec_file, lookback_period):
    self.logger.info("collect job execs")
    job_exec_writer = FileWriter(job_exec_file)
    query = """
            select  a.id as job_exec_id, a.name as job_name, j.id as flow_exec_id, a.status, a.user_retry_count,
            unix_timestamp(a.start_time) start_time, unix_timestamp(a.end_time) end_time,
            j.app_name as jname, j.app_path, transition from WF_ACTIONS a JOIN WF_JOBS j on a.wf_id = j.id where j.end_time > now() - INTERVAL %d MINUTE
            """ % (int(lookback_period))
    self.oz_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.oz_cursor)

    for row in rows:
      job_exec_record = OozieJobExecRecord(self.app_id,
                                           row['app_path'],
                                           row['flow_exec_id'],
                                           row['flow_exec_id'],
                                           row['job_name'],
                                           row['app_path'] + "/" + row['job_name'],
                                           row['job_exec_id'],
                                           row['status'],
                                           row['user_retry_count'],
                                           row['start_time'],
                                           row['end_time'],
                                           self.wh_exec_id)
      job_exec_writer.append(job_exec_record)
    job_exec_writer.close()

if __name__ == "__main__":
  props = sys.argv[1]
  az = OozieExtract(props)
  az.run()
