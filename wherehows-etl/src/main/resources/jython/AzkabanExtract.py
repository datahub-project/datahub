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
from wherehows.common.schemas import AzkabanFlowRecord
from wherehows.common.schemas import AzkabanJobRecord
from wherehows.common.schemas import AzkabanFlowDagRecord
from wherehows.common.schemas import AzkabanFlowExecRecord
from wherehows.common.schemas import AzkabanJobExecRecord
from wherehows.common.schemas import AzkabanFlowScheduleRecord
from wherehows.common.schemas import AzkabanFlowOwnerRecord
from wherehows.common.enums import AzkabanPermission
from wherehows.common.utils import AzkabanJobExecUtil
from wherehows.common import Constant
from wherehows.common.enums import SchedulerType
from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
import os, sys, json, gzip
import StringIO
import datetime, time
import DbUtil


class AzkabanExtract:

  _period_unit_table = {'d': 'DAY',
                        'M': 'MONTH',
                        'h': 'HOUR',
                        'm': 'MINUTE',
                        'w': 'WEEK'}

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.az_con = zxJDBC.connect(args[Constant.AZ_DB_URL_KEY],
                                 args[Constant.AZ_DB_USERNAME_KEY],
                                 args[Constant.AZ_DB_PASSWORD_KEY],
                                 args[Constant.AZ_DB_DRIVER_KEY])
    self.az_cursor = self.az_con.cursor()
    self.lookback_period = args[Constant.AZ_EXEC_ETL_LOOKBACK_MINS_KEY]
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(SchedulerType.AZKABAN) + "/" + str(self.app_id)

    if not os.path.exists(self.metadata_folder):
      try:
        os.makedirs(self.metadata_folder)
      except Exception as e:
        self.logger.error(e)

  def run(self):
    self.logger.info("Begin Azkaban Extract")
    try:
      self.collect_flow_jobs(self.metadata_folder + "/flow.csv", self.metadata_folder + "/job.csv", self.metadata_folder + "/dag.csv")
      self.collect_flow_owners(self.metadata_folder + "/owner.csv")
      self.collect_flow_schedules(self.metadata_folder + "/schedule.csv")
      self.collect_flow_execs(self.metadata_folder + "/flow_exec.csv", self.metadata_folder + "/job_exec.csv", self.lookback_period)
    finally:
      self.az_cursor.close()
      self.az_con.close()

  def collect_flow_jobs(self, flow_file, job_file, dag_file):
    self.logger.info("collect flow&jobs")
    query = "SELECT distinct f.*, p.name as project_name FROM  project_flows f inner join projects p on f.project_id = p.id and f.version = p.version where p.active = 1"
    self.az_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.az_cursor)
    flow_writer = FileWriter(flow_file)
    job_writer = FileWriter(job_file)
    dag_writer = FileWriter(dag_file)
    row_count = 0

    for row in rows:
      row['version'] = 0 if (row["version"] is None) else row["version"]

      json_column = 'json'
      unzipped_content = gzip.GzipFile(mode='r', fileobj=StringIO.StringIO(row[json_column].tostring())).read()
      try:
        row[json_column] = json.loads(unzipped_content)
      except:
        pass

      flow_path = row['project_name'] + ":" + row['flow_id']

      flow_record = AzkabanFlowRecord(self.app_id,
                                      row['flow_id'],
                                      row['project_name'],
                                      flow_path,
                                      0,
                                      row['modified_time'] / 1000,
                                      row["version"],
                                      'Y',
                                      self.wh_exec_id)
      flow_writer.append(flow_record)

      # get flow jobs
      nodes = row[json_column]['nodes']
      for node in nodes:
        job_record = AzkabanJobRecord(self.app_id,
                                      flow_path,
                                      row["version"],
                                      node['id'],
                                      flow_path + '/' + node['id'],
                                      node['jobType'],
                                      'Y',
                                      self.wh_exec_id)
        if node['jobType'] == 'flow':
          job_record.setRefFlowPath(row['project_name'] + ":" + node['embeddedFlowId'])
        job_writer.append(job_record)

      # job dag
      edges = row[json_column]['edges']
      for edge in edges:
        dag_edge = AzkabanFlowDagRecord(self.app_id,
                                        flow_path,
                                        row['version'],
                                        flow_path + '/' + edge['source'],
                                        flow_path + '/' + edge['target'],
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
    self.logger.info( "collect flow&job executions")
    flow_exec_writer = FileWriter(flow_exec_file)
    job_exec_writer = FileWriter(job_exec_file)

    cmd = """select * from execution_flows where end_time > UNIX_TIMESTAMP(now() - INTERVAL %d MINUTE) * 1000 """ % (int(look_back_period))
    self.az_cursor.execute(cmd)
    rows = DbUtil.dict_cursor(self.az_cursor)
    row_count = 0
    for row in rows:
      json_column = 'flow_data'
      unzipped_content = gzip.GzipFile(mode='r', fileobj=StringIO.StringIO(row[json_column].tostring())).read()
      try:
        row[json_column] = json.loads(unzipped_content)
      except Exception as e:
        self.logger.error(e)
        pass
      flow_data = row[json_column]
      flow_path = flow_data['projectName'] + ":" + flow_data['flowId']
      flow_exec_record = AzkabanFlowExecRecord(self.app_id,
                                               flow_data['flowId'],
                                               flow_path,
                                               row['version'],
                                               row['exec_id'],
                                               flow_data['status'],
                                               flow_data['attempt'],
                                               row['submit_user'],
                                               long(row['start_time']) / 1000,
                                               long(row['end_time']) / 1000,
                                               self.wh_exec_id)
      flow_exec_writer.append(flow_exec_record)
      nodes = flow_data['nodes']
      job_exec_records = []
      for node in nodes:
        job_exec_record = AzkabanJobExecRecord(self.app_id,
                                                flow_path,
                                                row['version'],
                                                row['exec_id'],
                                                node['id'],
                                                flow_path + "/" + node['id'],
                                                None,
                                                node['status'],
                                                node['attempt'],
                                                long(node['startTime']) / 1000,
                                                long(node['endTime']) / 1000,
                                                self.wh_exec_id)
        job_exec_records.append(job_exec_record)

      AzkabanJobExecUtil.sortAndSet(job_exec_records)
      for r in job_exec_records:
        job_exec_writer.append(r)

      row_count += 1
      if row_count % 10000 == 0:
        flow_exec_writer.flush()
        job_exec_writer.flush()
    flow_exec_writer.close()
    job_exec_writer.close()

  def collect_flow_schedules(self, schedule_file):
    # load flow scheduling info from table triggers
    self.logger.info("collect flow schedule")
    schedule_writer = FileWriter(schedule_file)
    query = "select * from triggers"
    self.az_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.az_cursor)
    for row in rows:
      json_column = 'data'
      if row[json_column] != None:
        unzipped_content = gzip.GzipFile(mode='r', fileobj=StringIO.StringIO(row[json_column].tostring())).read()
        try:
          row[json_column] = json.loads(unzipped_content)
        except Exception as e:
          self.logger.error(e)
          pass

        if not "projectId" in row[json_column]["actions"][0]["actionJson"]:
          continue
        # print json.dumps(row[json_column], indent=4)

        if row[json_column]["triggerCondition"]["checkers"][0]["checkerJson"]["isRecurring"] == 'true':
          unit, frequency, cron_expr = None, None, None
          period = row[json_column]["triggerCondition"]["checkers"][0]["checkerJson"]["period"]
          if period is not None and period != "null" and period[-1:] in self._period_unit_table:
            unit = self._period_unit_table[period[-1:]]
            frequency = int(row[json_column]["triggerCondition"]["checkers"][0]["checkerJson"]["period"][:-1])
          if "cronExpression" in row[json_column]["triggerCondition"]["checkers"][0]["checkerJson"]:
            cron_expr = row[json_column]["triggerCondition"]["checkers"][0]["checkerJson"]["cronExpression"]
          schedule_record = AzkabanFlowScheduleRecord(self.app_id,
                                                      row[json_column]["actions"][0]["actionJson"]["projectName"] + ':' + row[json_column]["actions"][0]["actionJson"]["flowName"],
                                                      unit,
                                                      frequency,
                                                      cron_expr,
                                                      long(row[json_column]["triggerCondition"]["checkers"][0]["checkerJson"]["firstCheckTime"]) / 1000,
                                                      int(time.mktime(datetime.date(2099,12,31).timetuple())),
                                                      '0',
                                                      self.wh_exec_id
                                                      )
          schedule_writer.append(schedule_record)
    schedule_writer.close()

  def collect_flow_owners(self, owner_file):
    # load user info from table project_permissions
    self.logger.info("collect owner&permissions")
    user_writer = FileWriter(owner_file)
    query = "select f.flow_id, p.name as project_name, p.version as project_verison, pp.name as owner, pp.permissions, pp.isGroup " \
            "from project_flows f join project_permissions pp on f.project_id = pp.project_id join projects p on f.project_id = p.id where p.active = 1"
    self.az_cursor.execute(query)
    rows = DbUtil.dict_cursor(self.az_cursor)

    for row in rows:
      record = AzkabanFlowOwnerRecord(self.app_id,
                                      row['project_name'] + ':' + row["flow_id"],
                                      row["owner"],
                                      AzkabanPermission(row["permissions"]).toFlatString(),
                                      'GROUP' if row['isGroup'] == 1 else 'LDAP',
                                      self.wh_exec_id)
      user_writer.append(record)
    user_writer.close()

if __name__ == "__main__":
  props = sys.argv[1]
  az = AzkabanExtract(props)
  az.run()
