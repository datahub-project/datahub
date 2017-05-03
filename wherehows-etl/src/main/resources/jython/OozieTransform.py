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

from jython.SchedulerTransform import SchedulerTransform
from wherehows.common.enums import SchedulerType
import sys


class OozieTransform(SchedulerTransform):
  def __init__(self, args):
    SchedulerTransform.__init__(self, args, SchedulerType.OOZIE)

  def read_dag_file_to_stg(self):
    SchedulerTransform.read_dag_file_to_stg(self)
    query = """
            UPDATE stg_flow_job sj
            SET sj.is_last = 'N'
            WHERE sj.job_type = ':FORK:' AND sj.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_flow_exec_file_to_stg(self):
    SchedulerTransform.read_flow_exec_file_to_stg(self)
    # Insert new flow execution into mapping table to generate flow exec id
    query = """
            INSERT INTO flow_execution_id_map (app_id, source_exec_uuid)
            SELECT sf.app_id, sf.flow_exec_uuid FROM stg_flow_execution sf
            WHERE sf.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM flow_execution_id_map where app_id = sf.app_id AND source_exec_uuid = sf.flow_exec_uuid)
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_execution stg
            JOIN flow_execution_id_map fm
            ON stg.app_id = fm.app_id AND stg.flow_exec_uuid = fm.source_exec_uuid
            SET stg.flow_exec_id = fm.flow_exec_id WHERE stg.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_job_exec_file_to_stg(self):
    SchedulerTransform.read_job_exec_file_to_stg(self)
    # Insert new job execution into mapping table to generate job exec id
    query = """
            INSERT INTO job_execution_id_map (app_id, source_exec_uuid)
            SELECT sj.app_id, sj.job_exec_uuid FROM stg_job_execution sj
            WHERE sj.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM job_execution_id_map where app_id = sj.app_id AND source_exec_uuid = sj.job_exec_uuid)
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_job_execution stg
            JOIN job_execution_id_map jm
            ON stg.app_id = jm.app_id AND stg.job_exec_uuid = jm.source_exec_uuid
            SET stg.job_exec_id = jm.job_exec_id WHERE stg.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_job_execution stg
            JOIN flow_execution_id_map fm
            ON stg.app_id = fm.app_id AND stg.flow_exec_uuid = fm.source_exec_uuid
            SET stg.flow_exec_id = fm.flow_exec_id WHERE stg.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()


if __name__ == "__main__":
  props = sys.argv[1]
  oz = OozieTransform(props)
  oz.run()
