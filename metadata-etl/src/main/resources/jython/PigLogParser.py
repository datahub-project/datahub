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
from wherehows.common.schemas import AppworxFlowRecord
from wherehows.common.schemas import AppworxJobRecord
from wherehows.common.schemas import AppworxFlowDagRecord
from wherehows.common.schemas import AppworxFlowExecRecord
from wherehows.common.schemas import AppworxJobExecRecord
from wherehows.common.schemas import AppworxFlowScheduleRecord
from wherehows.common.schemas import AppworxFlowOwnerRecord
from wherehows.common.enums import AzkabanPermission
from wherehows.common import Constant
from wherehows.common.enums import SchedulerType
from com.ziclix.python.sql import zxJDBC
from pyparsing import *
import ParseUtil
import os
import DbUtil
import sys
import gzip
import StringIO
import json
import datetime
import time
import re
from org.slf4j import LoggerFactory

class PigLogParser:

  def __init__(self, file_content=None, log_file_name=None):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.text = None

    if file_content is None and log_file_name is None:
      self.logger.error("Initialization error for Pig log parser")
      sys.exit(-1)

    if log_file_name is not None:
      self.log_file_name = log_file_name
      file_ext = os.path.splitext(self.log_file_name)[1][1:]
      if file_ext == 'gz':
        try:
          self.text = gzip.GzipFile(mode='r', filename=self.log_file_name).read()
        except:
          self.logger.error('exception')
          self.logger.error(str(sys.exc_info()[0]))
      else:
        self.text = open(self.log_file_name, 'r').read()
    else:
      self.text = file_content

  def simple_parser(self):
    source_target_map = {}

    connection_line = re.search(r'Connecting to hadoop file system at:\s+(.*)', self.text)
    if connection_line is not None:
      cluster = re.search(r'hdfs://(.*?)\-(.*?)\w{2}\d{2}\..*',connection_line.group(1))
      if cluster is not None:
        source_target_map['cluster'] = cluster.group(2).upper()
        source_target_map['hive-cluster'] = cluster.group(1).upper() + '-' + cluster.group(2).upper() + '-HIVE'
      else:
        source_target_map['cluster'] = 'Unknown'
        source_target_map['hive-cluster'] = 'Unknown'
    else:
        source_target_map['cluster'] = 'Unknown'
        source_target_map['hive-cluster'] = 'Unknown'

    search_obj = re.findall(r'Successfully\s+(read|stored)\s+(\d+)\s+records.*?[from|in]:\s+"(.*?)"', self.text, re.M)
    if len(search_obj) == 0:
      return {}

    for s in search_obj:
      table_type = 'source' if s[0] == 'read' else 'target'
      record_count = s[1]
      for rel in s[2].split(','):
        loc_attrib = ParseUtil.get_hdfs_location_abstraction(rel)
        if any(loc_attrib) == 0: continue
        freq = loc_attrib['frequency']
        abstracted_loc = loc_attrib['abstracted_path'] + \
                         ('/' + freq if loc_attrib['explicit_partition'] else '')
        start_partition = loc_attrib['start_partition']
        end_partition = loc_attrib['end_partition']
        format_mask = loc_attrib['format_mask']
        abstracted_hdfs_path = loc_attrib['abstracted_path']
        storage_type = loc_attrib['storage_type']

        if not source_target_map.has_key(abstracted_loc):
          source_target_map[abstracted_loc] = {}
          source_target_map[abstracted_loc] = \
              { 'partition_type' : freq, \
                'record_count' : int(record_count), \
                'table_type' : table_type , \
                'format_mask' : format_mask, \
                'abstracted_hdfs_path' : abstracted_hdfs_path, \
                'storage_type' : storage_type \
                }
          source_target_map[abstracted_loc]['start_partition'] = []
          source_target_map[abstracted_loc]['end_partition'] = []
          source_target_map[abstracted_loc]['hdfs_path'] = []
        else:
          source_target_map[abstracted_loc]['record_count'] = \
              source_target_map[abstracted_loc]['record_count'] + \
              int(record_count)

        if rel in source_target_map[abstracted_loc]['hdfs_path']: continue
        if start_partition is not None:
          source_target_map[abstracted_loc]['start_partition'].append(start_partition)
        if end_partition is not None:
          source_target_map[abstracted_loc]['end_partition'].append(end_partition)
        source_target_map[abstracted_loc]['hdfs_path'].append(rel)

    for k, v in source_target_map.items():
      if k == 'cluster' or k == 'hive-cluster': continue
      if len(source_target_map[k]['start_partition']) > 0:
        source_target_map[k]['min_start_partition'] = min(source_target_map[k]['start_partition'])
        source_target_map[k]['partition_count'] = len(source_target_map[k]['start_partition'])
      else:
        source_target_map[k]['min_start_partition'] = None
        if len(source_target_map[k]['end_partition']) > 0:
          source_target_map[k]['max_end_partition'] = max(source_target_map[k]['end_partition'])
        else:
          source_target_map[k]['max_end_partition'] = None

    # Special processing for sa_cbi - China BI. Here Pig script uses UDF to write data into HDFS using
    # a field inside the processed data to partition
    # Example: Output directory : '/projects/cbi/data/tracking/FollowEvent/*2015-03-03-16-54-21-268*'
    # This is a temporary directory. Data finally moves into daily folders (partitions) based on the
    # value(s) present in a given column
    # We do approximation here. We look at input directories read and use min / max partition of
    # input directory as attributes of output directory


    for k, v in source_target_map.items():
      if k != 'cluster' and k != 'hive-cluster' and v['table_type'] == 'source' and v['partition_type'] != 'snapshot':
        source_key = k
        break

    for k,v in source_target_map.items():
      if k != 'cluster' and k != 'hive-cluster' and v['table_type'] == 'target' and v['partition_type'] == 'snapshot':
        if v['hdfs_path'][0].split('/')[-1].startswith('*') and v['hdfs_path'][0].split('/')[-1].endswith('*'):
          if 'min_start_partition' in source_target_map[source_key]:
            v['min_start_partition'] = source_target_map[source_key]['min_start_partition']
          if 'max_end_partition' in source_target_map[source_key]:
            v['max_end_partition'] = source_target_map[source_key]['max_end_partition']
          if 'format_mask' in source_target_map[source_key]:
            v['format_mask'] = source_target_map[source_key]['format_mask']
          if 'start_partition' in source_target_map[source_key]:
            v['start_partition'] = source_target_map[source_key]['start_partition']
          if 'end_partition' in source_target_map[source_key]:
            v['end_partition'] = source_target_map[source_key]['end_partition']
          if 'partition_type' in source_target_map[source_key]:
            v['partition_type'] = source_target_map[source_key]['partition_type']
          if 'partition_count' in source_target_map[source_key]:
            v['partition_count'] = source_target_map[source_key]['partition_count']

    return source_target_map

if __name__ == "__main__":
  props = sys.argv[1]
  pig_parser = PigLogParser()
