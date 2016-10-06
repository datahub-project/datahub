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

from wherehows.common.enums import SchedulerType
from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
import sys
from org.slf4j import LoggerFactory


class SchedulerTransform:

  _tables = {"flows": {"columns": "app_id, flow_name, flow_path, flow_level, source_version, source_created_time, source_modified_time, wh_etl_exec_id",
                       "file": "flow.csv",
                       "table": "stg_flow"},
             "jobs": {"columns": "app_id, flow_path, source_version, job_name, job_path, job_type, wh_etl_exec_id",
                      "file": "job.csv",
                      "table": "stg_flow_job"},
             "dags": {"columns": "app_id, flow_path, source_version, source_job_path, target_job_path, wh_etl_exec_id",
                      "file": "dag.csv",
                      "table": "stg_flow_dag_edge"},
             "owners": {"columns": "app_id, flow_path, owner_id, wh_etl_exec_id",
                        "file": "owner.csv",
                        "table": "stg_flow_owner_permission"},
             "schedules": {"columns": "app_id, flow_path, unit, frequency, cron_expression, effective_start_time, effective_end_time, ref_id, wh_etl_exec_id",
                           "file": "schedule.csv",
                           "table": "stg_flow_schedule"},
             "flow_execs": {"columns": "app_id, flow_name, flow_path, flow_exec_uuid, source_version, flow_exec_status, attempt_id, executed_by, start_time, end_time, wh_etl_exec_id",
                            "file": "flow_exec.csv",
                            "table": "stg_flow_execution"},
             "job_execs": {"columns": "app_id, flow_path, flow_exec_uuid, source_version, job_name, job_path, job_exec_uuid, job_exec_status, attempt_id, start_time, end_time, wh_etl_exec_id",
                           "file": "job_exec.csv",
                           "table": "stg_job_execution"}
             }

  _read_file_template = """
                        LOAD DATA LOCAL INFILE '{folder}/{file}'
                        INTO TABLE {table}
                        FIELDS TERMINATED BY '\x1a' ESCAPED BY '\0'
                        LINES TERMINATED BY '\n'
                        ({columns});
                        """

  _get_flow_id_template = """
                          UPDATE {table} stg
                          JOIN flow_source_id_map fm
                          ON stg.app_id = fm.app_id AND stg.flow_path = fm.source_id_string
                          SET stg.flow_id = fm.flow_id WHERE stg.app_id = {app_id}
                          """

  _get_job_id_template = """
                          UPDATE {table} stg
                          JOIN job_source_id_map jm
                          ON stg.app_id = jm.app_id AND stg.job_path = jm.source_id_string
                          SET stg.job_id = jm.job_id WHERE stg.app_id = {app_id}
                          """

  _clear_staging_tempalte = """
                            DELETE FROM {table} WHERE app_id = {app_id}
                            """

  def __init__(self, args, scheduler_type):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(scheduler_type) + "/" + str(self.app_id)

  def run(self):
    try:
      self.read_flow_file_to_stg()
      self.read_job_file_to_stg()
      self.read_dag_file_to_stg()
      self.read_flow_owner_file_to_stg()
      self.read_flow_schedule_file_to_stg()
      self.read_flow_exec_file_to_stg()
      self.read_job_exec_file_to_stg()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def read_flow_file_to_stg(self):
    t = self._tables["flows"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Insert new flow into mapping table to generate flow id
    query = """
            INSERT INTO flow_source_id_map (app_id, source_id_string)
            SELECT sf.app_id, sf.flow_path FROM {table} sf
            WHERE sf.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM flow_source_id_map where app_id = sf.app_id AND source_id_string = sf.flow_path)
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_job_file_to_stg(self):
    t = self._tables["jobs"]

    # Clearing stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # ad hoc fix for null values, need better solution by changing the load script
    query = """
            UPDATE {table} stg
            SET stg.ref_flow_path = null
            WHERE stg.ref_flow_path = 'null' and stg.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update sub flow id from mapping table
    query = """
            UPDATE {table} stg
            JOIN flow_source_id_map fm
            ON stg.app_id = fm.app_id AND stg.ref_flow_path = fm.source_id_string
            SET stg.ref_flow_id = fm.flow_id WHERE stg.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Insert new job into job map to generate job id
    query = """
            INSERT INTO job_source_id_map (app_id, source_id_string)
            SELECT sj.app_id, sj.job_path FROM {table} sj
            WHERE sj.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM job_source_id_map where app_id = sj.app_id AND source_id_string = sj.job_path)
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update job id from mapping table
    query = self._get_job_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update job type id from job type reverse map
    query = """
            UPDATE {table} sj
            JOIN cfg_job_type_reverse_map jtm
            ON sj.job_type = jtm.job_type_actual
            SET sj.job_type_id = jtm.job_type_id
            WHERE sj.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_dag_file_to_stg(self):
    t = self._tables["dags"]

    # Clearing staging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update source_job_id
    query = """
            UPDATE {table} sj JOIN job_source_id_map jm ON sj.app_id = jm.app_id AND sj.source_job_path = jm.source_id_string
            SET sj.source_job_id = jm.job_id
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update target_job_id
    query = """
            UPDATE {table} sj JOIN job_source_id_map jm ON sj.app_id = jm.app_id AND sj.target_job_path = jm.source_id_string
            SET sj.target_job_id = jm.job_id
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update pre_jobs and post_jobs in stg_flow_jobs table
    # need increase group concat max length to avoid overflow
    query = "SET group_concat_max_len=40960"
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj JOIN
            (SELECT source_job_id as job_id, source_version, GROUP_CONCAT(distinct target_job_id SEPARATOR ',') as post_jobs
            FROM {table} WHERE app_id = {app_id} AND source_job_id != target_job_id
            GROUP BY source_job_id, source_version) as d
            ON sj.job_id = d.job_id AND sj.source_version = d.source_version
            SET sj.post_jobs = d.post_jobs
            WHERE sj.app_id = {app_id};
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj JOIN
            (SELECT target_job_id as job_id, source_version, GROUP_CONCAT(distinct source_job_id SEPARATOR ',') as pre_jobs
            FROM {table} WHERE app_id = {app_id} AND source_job_id != target_job_id
            GROUP BY target_job_id, source_version) as d
            ON sj.job_id = d.job_id AND sj.source_version = d.source_version
            SET sj.pre_jobs = d.pre_jobs
            WHERE sj.app_id = {app_id};
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj
            SET sj.is_first = 'Y'
            WHERE sj.pre_jobs IS NULL AND sj.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj
            SET sj.is_last = 'Y'
            WHERE sj.post_jobs IS NULL AND sj.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = self._clear_staging_tempalte.format(table="stg_flow_dag", app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            INSERT INTO stg_flow_dag (app_id, flow_id, source_version, wh_etl_exec_id, dag_md5)
            SELECT DISTINCT app_id, flow_id, source_version, wh_etl_exec_id, md5(group_concat(source_job_id, "-", target_job_id ORDER BY source_job_id,target_job_id SEPARATOR ",")) dag_md5
            FROM {table} t
            WHERE app_id = {app_id} GROUP BY app_id, flow_id, source_version, wh_etl_exec_id
            UNION
            SELECT DISTINCT app_id, flow_id, source_version, wh_etl_exec_id, 0
            FROM stg_flow sf
            WHERE sf.app_id = {app_id} AND NOT EXISTS (SELECT * FROM {table} t WHERE t.app_id = sf.app_id AND t.flow_id = sf.flow_id AND t.source_version = sf.source_version)
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_dag s
            LEFT JOIN flow_dag f
            ON s.app_id = f.app_id AND s.flow_id = f.flow_id AND (f.is_current IS NULL OR f.is_current = 'Y')
            SET s.dag_version = CASE WHEN f.dag_md5 IS NULL THEN 0 WHEN s.dag_md5 != f.dag_md5 THEN f.dag_version + 1 ELSE f.dag_version END
            WHERE s.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj
            JOIN
            (SELECT DISTINCT app_id, flow_id, source_version, dag_version FROM stg_flow_dag) dag
            ON sj.app_id = dag.app_id AND sj.flow_id = dag.flow_id AND sj.source_version = dag.source_version
            SET sj.dag_version = dag.dag_version
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)

    self.wh_con.commit()

  def read_flow_owner_file_to_stg(self):
    t = self._tables["owners"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_flow_schedule_file_to_stg(self):
    t = self._tables["schedules"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow is_scheduled flag
    query = """
            UPDATE stg_flow f
            LEFT JOIN {table} fs
            ON f.flow_id = fs.flow_id AND f.app_id = fs.app_id
            SET f.is_scheduled = CASE WHEN fs.flow_id IS NULL THEN 'N' ELSE 'Y' END
            WHERE f.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_flow_exec_file_to_stg(self):
    t = self._tables["flow_execs"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_job_exec_file_to_stg(self):
    t = self._tables["job_execs"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update job id from mapping table
    query = self._get_job_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()


if __name__ == "__main__":
  props = sys.argv[1]
  st = SchedulerTransform(props, SchedulerType.GENERIC)
  st.run()
