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
import sys
from org.slf4j import LoggerFactory


class SchedulerLoad:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.JOB_REF_ID_KEY])
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY], args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY], args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()

  def run(self):
    try:
      self.load_flows()
      self.load_jobs()
      self.load_flow_dags()
      self.load_flow_schedules()
      self.load_flow_owner_permissions()
      self.load_flow_executions()
      self.load_job_executions()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def load_flows(self):
    cmd = """
          INSERT INTO flow (app_id, flow_id, flow_name, flow_group, flow_path, flow_level, source_created_time, source_modified_time, source_version,
          is_active, is_scheduled, created_time, modified_time, wh_etl_exec_id)
          SELECT app_id, flow_id, flow_name, flow_group, flow_path, flow_level, source_created_time, source_modified_time, source_version,
          is_active, is_scheduled, unix_timestamp(NOW()) created_time, NULL modified_time, wh_etl_exec_id
          FROM stg_flow s
          WHERE s.app_id = {app_id}
          ON DUPLICATE KEY UPDATE
          flow_name = s.flow_name,
          flow_group = s.flow_group,
          flow_path = s.flow_path,
          flow_level = s.flow_level,
          source_created_time = s.source_created_time,
          source_version = s.source_version,
          is_active = s.is_active,
          is_scheduled = s.is_scheduled,
          modified_time = unix_timestamp(NOW()),
          wh_etl_exec_id = s.wh_etl_exec_id
          """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

  def load_jobs(self):
    cmd = """
            UPDATE flow_job j
            LEFT JOIN stg_flow_job s
            ON j.app_id = s.app_id AND j.job_id = s.job_id
            SET j.is_current = 'N'
            WHERE (s.job_id IS NULL OR s.dag_version > j.dag_version) AND j.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

    cmd = """
          INSERT INTO flow_job (app_id, flow_id, first_source_version, dag_version, job_id, job_name, job_path, job_type_id, job_type, ref_flow_id, pre_jobs, post_jobs,
          is_current, is_first, is_last, created_time, modified_time, wh_etl_exec_id)
          SELECT app_id, flow_id, source_version first_source_version, dag_version, job_id, job_name, job_path, job_type_id, job_type, ref_flow_id, pre_jobs, post_jobs,
          'Y', is_first, is_last, unix_timestamp(NOW()) created_time, NULL, wh_etl_exec_id
          FROM stg_flow_job s
          WHERE s.app_id = {app_id}
          ON DUPLICATE KEY UPDATE
          flow_id = s.flow_id,
          last_source_version = case when s.source_version = first_source_version and last_source_version is NULL then NULL else s.source_version end,
          job_name = s.job_name,
          job_path = s.job_path,
          job_type_id = s.job_type_id,
          job_type = s.job_type,
          ref_flow_id = s.ref_flow_id,
          pre_jobs = s.pre_jobs,
          post_jobs = s.post_jobs,
          is_current = 'Y',
          is_first = s.is_first,
          is_last = s.is_last,
          modified_time = unix_timestamp(NOW()),
          wh_etl_exec_id = s.wh_etl_exec_id
          """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

  def load_flow_dags(self):
    cmd = """
          UPDATE flow_dag f
          SET is_current = 'N'
          WHERE f.app_id = {app_id}
          """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)

    cmd = """
          INSERT INTO flow_dag (app_id, flow_id, source_version, dag_version, dag_md5, is_current, wh_etl_exec_id)
          SELECT app_id, flow_id, source_version, dag_version, dag_md5, 'Y', wh_etl_exec_id
          FROM stg_flow_dag s
          WHERE s.app_id = {app_id}
          ON DUPLICATE KEY UPDATE
          dag_md5 = s.dag_md5,
          dag_version = s.dag_version,
          is_current = 'Y',
          wh_etl_exec_id = s.wh_etl_exec_id
          """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

  def load_flow_schedules(self):
    cmd = """
          UPDATE flow_schedule
          SET is_active = 'N'
          WHERE app_id = {app_id}
          """.format(app_id=self.app_id)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

    cmd = """
          INSERT INTO flow_schedule (app_id, flow_id, unit, frequency, cron_expression, included_instances, excluded_instances, effective_start_time, effective_end_time, is_active, ref_id,
           created_time, modified_time, wh_etl_exec_id)
          SELECT app_id, flow_id, unit, frequency, cron_expression, included_instances, excluded_instances, effective_start_time, effective_end_time, 'Y', ref_id,
          unix_timestamp(NOW()) created_time, NULL modified_time, wh_etl_exec_id
          FROM stg_flow_schedule s
          WHERE s.app_id = {app_id} AND s.flow_id IS NOT NULL
          ON DUPLICATE KEY UPDATE
          unit = s.unit,
          frequency = s.frequency,
          cron_expression = s.cron_expression,
          is_active = 'Y',
          ref_id = s.ref_id,
          included_instances = s.included_instances,
          excluded_instances = s.excluded_instances,
          effective_start_time = s.effective_start_time,
          effective_end_time = s.effective_end_time,
          modified_time = unix_timestamp(NOW()),
          wh_etl_exec_id = s.wh_etl_exec_id
          """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

  def load_flow_owner_permissions(self):
    cmd = """
          INSERT INTO flow_owner_permission (app_id, flow_id, owner_id, permissions, owner_type,
           created_time,  modified_time, wh_etl_exec_id)
          SELECT app_id, flow_id, owner_id, permissions, owner_type,
          unix_timestamp(NOW()) created_time, NULL modified_time, wh_etl_exec_id
          FROM stg_flow_owner_permission s
          WHERE s.app_id = {app_id} AND s.flow_id IS NOT NULL
          ON DUPLICATE KEY UPDATE
          permissions = s.permissions,
          owner_type = s.owner_type,
          modified_time = unix_timestamp(NOW()),
          wh_etl_exec_id = s.wh_etl_exec_id
          """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

  def load_flow_executions(self):
    cmd = """INSERT INTO flow_execution
              (app_id, flow_id, flow_name, source_version, flow_exec_id, flow_exec_uuid, flow_exec_status, attempt_id, executed_by,
               start_time, end_time, created_time, modified_time, wh_etl_exec_id)
               SELECT app_id, flow_id, flow_name, source_version, flow_exec_id, flow_exec_uuid, flow_exec_status, attempt_id, executed_by,
               case when start_time = 0 then null else start_time end, case when end_time = 0 then null else end_time end, unix_timestamp(NOW()), NULL, wh_etl_exec_id
               FROM stg_flow_execution s
               WHERE s.app_id = {app_id} AND s.flow_id IS NOT NULL
               ON DUPLICATE KEY UPDATE
               source_version = s.source_version,
               flow_exec_uuid = s.flow_exec_uuid,
               flow_exec_status = s.flow_exec_status,
               start_time = case when s.start_time = 0 then null else s.start_time end,
               end_time = case when s.end_time = 0 then null else s.end_time end,
               modified_time = unix_timestamp(NOW()),
               wh_etl_exec_id = s.wh_etl_exec_id
               """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()

  def load_job_executions(self):
    cmd = """INSERT INTO job_execution
              (app_id, flow_id, source_version, flow_exec_id, job_id, job_name, job_exec_id, job_exec_uuid, job_exec_status, attempt_id,
               start_time, end_time, created_time, modified_time, wh_etl_exec_id)
              SELECT app_id, flow_id, source_version, flow_exec_id, job_id, job_name, job_exec_id, job_exec_uuid, job_exec_status, attempt_id,
               case when start_time = 0 then null else start_time end, case when end_time = 0 then null else end_time end, unix_timestamp(NOW()), NULL, wh_etl_exec_id
               FROM stg_job_execution s
               WHERE s.app_id = {app_id} AND s.job_id IS NOT NULL AND s.flow_id IS NOT NULL
               ON DUPLICATE KEY UPDATE
               source_version = s.source_version,
               attempt_id = s.attempt_id,
               flow_exec_id = s.flow_exec_id,
               job_exec_uuid = s.job_exec_uuid,
               job_exec_status = s.job_exec_status,
               start_time = case when s.start_time = 0 then null else s.start_time end,
               end_time = case when s.end_time = 0 then null else s.end_time end,
               modified_time = unix_timestamp(NOW()),
               wh_etl_exec_id = s.wh_etl_exec_id
               """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()


if __name__ == "__main__":
  props = sys.argv[1]
  oz = SchedulerLoad(props)
  oz.run()
