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

import json
import datetime
import sys, os
import time
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import DatasetSchemaRecord, DatasetFieldRecord
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class TeradataTransform:
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def transform(self, input, td_metadata, td_field_metadata):
    '''
    convert from json to csv
    :param input: input json file
    :param td_metadata: output data file for teradata metadata
    :param td_field_metadata: output data file for teradata field metadata
    :return:
    '''
    f_json = open(input)
    data = json.load(f_json)
    f_json.close()

    schema_file_writer = FileWriter(td_metadata)
    field_file_writer = FileWriter(td_field_metadata)

    for d in data:
      i = 0
      for k in d.keys():
        if k not in ['tables', 'views']:
          continue
        self.logger.info("%s %4d %s" % (datetime.datetime.now().strftime("%H:%M:%S"), len(d[k]), k))
        for t in d[k]:
          self.logger.info("%4d %s" % (i, t['name']))
          if t['name'] == 'HDFStoTD_2464_ERR_1':
            continue
          i += 1
          output = {}
          prop_json = {}
          output['name'] = t['name']
          output['original_name'] = t['original_name']

          prop_json["createTime"] = t["createTime"] if t.has_key("createTime") else None
          prop_json["lastAlterTime"] = t["lastAlterTime"] if t.has_key("lastAlterTime") else None
          prop_json["lastAccessTime"] = t["lastAccessTime"] if t.has_key("lastAccessTime") else None
          prop_json["accessCount"] = t["accessCount"] if t.has_key("accessCount") else None
          prop_json["sizeInMbytes"] = t["sizeInMbytes"] if t.has_key("sizeInMbytes") else None
          if "type" in t:
            prop_json["storage_type"] = t["type"]
          if "partition" in t:
            prop_json["partition"] = t["partition"]
          if "partitions" in t:
            prop_json["partitions"] = t["partitions"]
          if "hashKey" in t:
            prop_json["hashKey"] = t["hashKey"]
          if "indices" in t:
            prop_json["indices"] = t["indices"]
          if "referenceTables" in t:
            prop_json["referenceTables"] = t["referenceTables"]
          if "viewSqlText" in t:
            prop_json["viewSqlText"] = t["viewSqlText"]

          output['fields'] = []
          flds = {}
          field_detail_list = []
          sort_id = 0
          for c in t['columns']:
            # output['fields'].append(
            #                    { 'name' : t['name'].encode('latin-1'),
            #                      'type' : None if c['data_type'] is None else c['data_type'].encode('latin-1'),
            #                      'attributes_json' : c}
            #                output['fields'][c['name'].encode('latin-1')].append({ "doc" : "", "type" : [None if c['data_type'] is None else c['data_type'].encode('latin-1')]})
            sort_id += 1
            output['fields'].append({"name": c['name'], "doc": '', "type": c['dataType'] if c['dataType'] else None,
                                     "nullable": c['nullable'], "maxByteLength": c['maxByteLength'],
                                     "format": c['columnFormat'] if c.has_key('columnFormat') else None,
                                     "accessCount": c['accessCount'] if c.has_key('accessCount') else None,
                                     "lastAccessTime": c['lastAccessTime'] if c.has_key("lastAccessTime") else None})

            flds[c['name']] = {'type': c['dataType'], "maxByteLength": c['maxByteLength']}

            field_detail_list.append(
              ["teradata:///%s/%s" % (d['database'], output['name']), str(sort_id), '0', '', c['name'], '',
               c['dataType'] if 'dataType' in c and c['dataType'] is not None else '',
               str(c['maxByteLength']) if 'maxByteLength' in c else '0',
               str(c['precision']) if 'precision' in c and c['precision'] is not None else '',
               str(c['scale']) if 'scale' in c and c['scale'] is not None else '',
               c['nullable'] if 'nullable' in c and c['nullable'] is not None else 'Y', '', '', '', '', '', '', ''])

          dataset_scehma_record = DatasetSchemaRecord(output['name'], json.dumps(output), json.dumps(prop_json),
                                                      json.dumps(flds),
                                                      "teradata:///%s/%s" % (d['database'], output['name']), 'Teradata',
                                                      output['original_name'],
                                                      (self.convert_timestamp(t["createTime"]) if t.has_key("createTime") else None),
                                                      (self.convert_timestamp(t["lastAlterTime"]) if t.has_key("lastAlterTime") else None))
          schema_file_writer.append(dataset_scehma_record)

          for fields in field_detail_list:
            field_record = DatasetFieldRecord(fields)
            field_file_writer.append(field_record)

        schema_file_writer.flush()
        field_file_writer.flush()
        self.logger.info("%20s contains %6d %s" % (d['database'], i, k))

    schema_file_writer.close()
    field_file_writer.close()

  def convert_timestamp(self, time_string):
    return int(time.mktime(time.strptime(time_string, "%Y-%m-%d %H:%M:%S")))


if __name__ == "__main__":
  args = sys.argv[1]
  t = TeradataTransform()
  t.log_file = args['teradata.log']

  t.transform(args[Constant.TD_SCHEMA_OUTPUT_KEY], args[Constant.TD_METADATA_KEY], args[Constant.TD_FIELD_METADATA_KEY])

