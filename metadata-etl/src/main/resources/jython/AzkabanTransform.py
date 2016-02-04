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

class AzkabanTransform(SchedulerTransform):
  SchedulerTransform._tables["flows"]["columns"] = "app_id, flow_name, flow_group, flow_path, flow_level, source_modified_time, source_version, is_active, wh_etl_exec_id"
  SchedulerTransform._tables["jobs"]["columns"] = "app_id, flow_path, source_version, job_name, job_path, job_type, ref_flow_path, is_current, wh_etl_exec_id"
  SchedulerTransform._tables["owners"]["columns"] = "app_id, flow_path, owner_id, permissions, owner_type, wh_etl_exec_id"
  SchedulerTransform._tables["flow_execs"]["columns"] = "app_id, flow_name, flow_path, source_version, flow_exec_id, flow_exec_status, attempt_id, executed_by, start_time, end_time, wh_etl_exec_id"
  SchedulerTransform._tables["job_execs"]["columns"] = "app_id, flow_path, source_version, flow_exec_id, job_name, job_path, job_exec_id, job_exec_status, attempt_id, start_time, end_time, wh_etl_exec_id"

  def __init__(self, args):
    SchedulerTransform.__init__(self, args, SchedulerType.AZKABAN)

if __name__ == "__main__":
  props = sys.argv[1]
  az = AzkabanTransform(props)
  az.run()
