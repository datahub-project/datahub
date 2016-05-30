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

import calendar
import json
import shutil
import sys
from com.ziclix.python.sql import zxJDBC
from datetime import datetime
from wherehows.common import Constant

from jython.ElasticSearchIndex import ElasticSearchIndex


class FlowTreeBuilder:
  def __init__(self, args):
    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    jdbc_driver = args[Constant.WH_DB_DRIVER_KEY]
    jdbc_url = args[Constant.WH_DB_URL_KEY]
    conn_mysql = zxJDBC.connect(jdbc_url, username, password, jdbc_driver)
    cur = conn_mysql.cursor()
    try:
      query = "select distinct f.flow_id, f.flow_name, f.flow_group, ca.app_code from flow f join cfg_application ca on f.app_id = ca.app_id order by app_code, flow_name"
      cur.execute(query)
      flows = cur.fetchall()
      self.flow_dict = dict()
      for flow in flows:
        current = self.flow_dict
        # if needed, use flow[3].replace(' ', '.')
        current = current.setdefault(flow[3], {})
        if flow[2] is not None:
          current = current.setdefault(flow[2], {})
        # for oozie
        else:
          current = current.setdefault('NA', {})

        current = current.setdefault(flow[1], {})
        current["__ID_OF_FLOW__"] = flow[0]
      self.file_name = args[Constant.FLOW_TREE_FILE_NAME_KEY]
      self.value = []
    finally:
      cur.close()
      conn_mysql.close()

  def build_trie_helper(self, depth, current, current_dict):
    nodes = []
    child_nodes = []
    for child_key in sorted(current_dict.keys()):
      if child_key == "__ID_OF_FLOW__":
        # Find a leaf node
        nodes.append({'title': current, 'level': depth, 'id': current_dict["__ID_OF_FLOW__"], 'children': [], 'folder': 0})
      else:
        child_nodes.extend(self.build_trie_helper(depth + 1, child_key, current_dict[child_key]))
    if len(child_nodes) != 0:
      nodes.append({'title': current, 'level': depth, 'children': child_nodes, 'folder': 1})
    return nodes

  def build_trie(self):
    for top_key in sorted(self.flow_dict.keys()):
      self.value.extend(self.build_trie_helper(1, top_key, self.flow_dict[top_key]))

  def write_to_file(self):
    tmp_file_name = self.file_name + ".tmp"
    f = open(tmp_file_name, "w")
    f.write(json.dumps({'children': self.value}))
    f.close()
    shutil.move(tmp_file_name, self.file_name)

  def run(self):
    self.build_trie()
    self.write_to_file()


def saveTreeInElasticSearchIfApplicable(args):
  es_url = args.get(Constant.WH_ELASTICSEARCH_URL_KEY, None)
  es_port = args.get(Constant.WH_ELASTICSEARCH_PORT_KEY, None)
  if es_url and es_port:
    esi = ElasticSearchIndex(args)
    d = datetime.utcnow()
    unixtime = calendar.timegm(d.utctimetuple())
    esi.update_flow_jobs(unixtime)

if __name__ == "__main__":
  ftb = FlowTreeBuilder(sys.argv[1])
  ftb.run()
  saveTreeInElasticSearchIfApplicable(sys.argv[1])
