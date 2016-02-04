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

from jython.SchedulerLoad import SchedulerLoad
import sys


class AzkabanLoad(SchedulerLoad):
  def __init__(self, args):
    SchedulerLoad.__init__(self, args)

  def load_flows(self):
    # set flows that not in staging table to inactive
    cmd = """
          UPDATE flow f
          LEFT JOIN stg_flow s
          ON f.app_id = s.app_id AND f.flow_id = s.flow_id
          SET f.is_active = 'N'
          WHERE s.flow_id IS NULL AND f.app_id = {app_id}
          """.format(app_id=self.app_id)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()
    SchedulerLoad.load_flows(self)


if __name__ == "__main__":
  props = sys.argv[1]
  az = AzkabanLoad(props)
  az.run()
