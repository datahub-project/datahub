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
from wherehows.common import Constant
from datetime import datetime

from jython.ElasticSearchIndex import ElasticSearchIndex


class DatasetTreeBuilder:
  def __init__(self, args):
    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    jdbc_driver = args[Constant.WH_DB_DRIVER_KEY]
    jdbc_url = args[Constant.WH_DB_URL_KEY]
    conn_mysql = zxJDBC.connect(jdbc_url, username, password, jdbc_driver)
    cur = conn_mysql.cursor()
    try:
      query = "select distinct id, concat(SUBSTRING_INDEX(urn, ':///', 1), '/', SUBSTRING_INDEX(urn, ':///', -1)) p from dict_dataset order by urn"
      cur.execute(query)
      datasets = cur.fetchall()
      self.dataset_dict = dict()
      for dataset in datasets:
        current = self.dataset_dict
        path_arr = dataset[1].split('/')
        for name in path_arr:
          current = current.setdefault(name, {})
        current["__ID_OF_DATASET__"] = dataset[0]
      self.file_name = args[Constant.DATASET_TREE_FILE_NAME_KEY]
      self.value = []
    finally:
      cur.close()
      conn_mysql.close()

  def build_trie_helper(self, depth, path, current, current_dict):
    nodes = []
    child_nodes = []
    for child_key in sorted(current_dict.keys()):
      if child_key == "__ID_OF_DATASET__":
        # Find a leaf node
        nodes.append({'title': current, 'level': depth, 'path': path, 'id': current_dict["__ID_OF_DATASET__"], 'children': [], 'folder': 0})
      else:
        delimiter = '/'
        if depth == 1:
          delimiter = ':///'
        child_nodes.extend(self.build_trie_helper(depth + 1, path + delimiter + child_key, child_key, current_dict[child_key]))
    if len(child_nodes) != 0:
      nodes.append({'title': current, 'level': depth, 'path': path, 'children': child_nodes, 'folder': 1})
    return nodes

  def build_trie(self):
    for top_key in sorted(self.dataset_dict.keys()):
      self.value.extend(self.build_trie_helper(1, top_key, top_key, self.dataset_dict[top_key]))

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
    esi.update_dataset(unixtime)

if __name__ == "__main__":
  d = DatasetTreeBuilder(sys.argv[1])
  d.run()
  saveTreeInElasticSearchIfApplicable(sys.argv[1])
