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
from org.slf4j import LoggerFactory
from wherehows.common import Constant

from jython.ElasticSearchIndex import ElasticSearchIndex


class DatasetTreeBuilder:
  def __init__(self, args):
    username = args[Constant.WH_DB_USERNAME_KEY]
    password = args[Constant.WH_DB_PASSWORD_KEY]
    jdbc_driver = args[Constant.WH_DB_DRIVER_KEY]
    jdbc_url = args[Constant.WH_DB_URL_KEY]
    self.conn_mysql = zxJDBC.connect(jdbc_url, username, password, jdbc_driver)
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

    cursor = self.conn_mysql.cursor()
    try:
      query = "SELECT DISTINCT id, concat(SUBSTRING_INDEX(urn, ':///', 1), '/', SUBSTRING_INDEX(urn, ':///', -1)) p, urn FROM dict_dataset ORDER BY urn"
      cursor.execute(query)
      datasets = cursor.fetchall()
      self.dataset_dict = dict()
      for dataset in datasets:
        current = self.dataset_dict
        path_arr = dataset[1].split('/')
        for name in path_arr:
          current = current.setdefault(name, {})
        current["__ID_OF_DATASET__"] = dataset[0]
      self.file_name = args.get(Constant.DATASET_TREE_FILE_NAME_KEY, None)
      self.value = []
    finally:
      cursor.close()

  def close_database_connection(self):
    if self.conn_mysql is not None:
      self.conn_mysql.close()

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

  def store_trie_in_database(self):
    cursor = self.conn_mysql.cursor()
    try:
      table_name = 'cfg_ui_trees'
      dataset_json = json.dumps({'children': self.value})

      updateStatement = "UPDATE %s SET value = '%s' WHERE name = 'datasets'" % (table_name, dataset_json)
      cursor.execute(updateStatement)

      self.conn_mysql.commit()
      self.logger.debug("Datasets trie stored in database")
    finally:
      cursor.close()

  def write_trie_to_file(self):
    tmp_file_name = self.file_name + ".tmp"
    f = open(tmp_file_name, "w")
    f.write(json.dumps({'children': self.value}))
    f.close()
    shutil.move(tmp_file_name, self.file_name)

  def save_trie(self):
    if self.file_name is None or self.file_name == '':
      self.store_trie_in_database()
    else:
      self.write_trie_to_file()

  def run(self):
    self.build_trie()
    self.save_trie()


def saveTreeInElasticSearchIfApplicable(args):
  es_url = args.get(Constant.WH_ELASTICSEARCH_URL_KEY, None)
  es_port = args.get(Constant.WH_ELASTICSEARCH_PORT_KEY, None)
  if es_url is not None and es_port is not None:
    esi = ElasticSearchIndex(args)
    d = datetime.utcnow()
    unixtime = calendar.timegm(d.utctimetuple())
    esi.update_dataset(unixtime)


if __name__ == "__main__":
  datasetTreeBuilder = DatasetTreeBuilder(sys.argv[1])
  try:
    datasetTreeBuilder.run()
  finally:
    datasetTreeBuilder.close_database_connection()
  saveTreeInElasticSearchIfApplicable(sys.argv[1])
