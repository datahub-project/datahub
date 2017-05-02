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
import csv, sys
import re
from org.slf4j import LoggerFactory
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import DatasetSchemaRecord, DatasetFieldRecord
from wherehows.common import Constant
from AvroColumnParser import AvroColumnParser


class HdfsTransform:
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def transform(self, raw_metadata, metadata_output, field_metadata_output):

    # sys.setdefaultencoding("UTF-8")

    input_json_file = open(raw_metadata, 'r')
    schema_file_writer = FileWriter(metadata_output)
    field_file_writer = FileWriter(field_metadata_output)
    i = 0
    self.sort_id = 0
    o_urn = ''
    p = ''

    for line in input_json_file:
      try:
        j = json.loads(line)
      except:
        self.logger.error("    Invalid JSON:\n%s" % line)
        continue

      i += 1
      o_field_list_ = []
      parent_field_path = ''
      self.sort_id = 0

      if not (j.has_key('attributes_json') or j.has_key('attributes')):
        o_properties = {"doc": null}
      else:
        o_properties = {}
        if j.has_key('attributes_json'):
          o_properties = json.loads(j['attributes_json'])
          del j['attributes_json']
        if j.has_key('attributes'):
          o_properties = dict(j['attributes'].items() + o_properties.items())
          del j['attributes']

      if j.has_key('uri'):
        o_urn = j['uri']
      elif o_properties.has_key('uri'):
        o_urn = o_properties['uri']
      else:
        self.logger.info('*** Warning: "uri" is not found in %s' % j['name'])
        o_urn = ''

      if o_urn.find('hdfs://') == 0:
        o_name = o_urn[o_urn.rfind('/') + 1:]
      elif o_properties.has_key('table_name'):
        o_name = o_properties['table_name']
      elif j.has_key('name') and j['name'][0:5] != 'TUPLE':
        o_name = j['name']
      else:
        o_name = o_urn[o_urn.rfind('/') + 1:]

      if j.has_key('id') or not j.has_key('fields'):  # esWritable schema
        o_fields = {}
        for k in j:
          if not (k == 'uri' or k == 'attributes' or k == 'doc'):
            if type(j[k]) == list:
              o_fields[k] = {"name": k, "type": 'list', "doc": str(j[k])}
            elif type(j[k]) == dict:
              o_fields[k] = {"name": k, "type": 'dict', "doc": str(j[k])}
            else:
              o_fields[k] = {"name": k, "type": j[k], "doc": None}

            self.sort_id += 1
            o_field_list_.append([o_urn, self.sort_id, 0, '', k, o_fields[k]['type'], '', '', '',
                                  o_fields[k]['doc'].replace("\n", ' ') if o_fields[k]['doc'] is not None else None])

      elif j.has_key('fields'):
        o_fields = {}
        for f in j['fields']:
          o_field_name = f['name']
          o_fields[o_field_name] = dict(f)  # for schema output
          if f.has_key('attributes_json'):
            f['attributes'] = json.loads(f['attributes_json'])
            del f['attributes_json']

        acp = AvroColumnParser(j, o_urn)
        o_field_list_ += acp.get_column_list_result()

      else:
        o_fields = {"doc": None}

      if j.has_key('attributes') and not o_properties.has_key('source'):
        o_properties['source'] = j['attributes']['source']

      if o_urn.startswith('hdfs:///') and self.file_regex_source_map is not None:
        o_source = self.get_source(o_urn[7:])
      else:
        self.logger.warn("property : " + Constant.HDFS_FILE_SOURCE_MAP_KEY +
                         " is None, will use default source for all dataset")
        o_source = 'Hdfs'

      self.logger.info(
        "%4i (%6i): %4i fields, %4i total fields(including nested) found in [%s]@%s with source %s" % (i, len(j), len(o_fields), len(o_field_list_), o_name, o_urn, o_source))

      dataset_schema_record = DatasetSchemaRecord(o_name, json.dumps(j, sort_keys=True),
                                                  json.dumps(o_properties, sort_keys=True), json.dumps(o_fields), o_urn,
                                                  o_source, None, None, None)
      schema_file_writer.append(dataset_schema_record)

      for fields in o_field_list_:
        field_record = DatasetFieldRecord(fields)
        field_file_writer.append(field_record)

    schema_file_writer.close()
    field_file_writer.close()
    input_json_file.close()

  def get_source(self, file_location):
    # derived the source from o_name
    source = 'Hdfs'  # default Hdfs
    for entry in self.file_regex_source_map:
      key, value = list(entry.items())[0]
      if re.match(key, file_location):
        source = value
        break
    return source


if __name__ == "__main__":
  args = sys.argv[1]

  t = HdfsTransform()
  if args.has_key(Constant.HDFS_FILE_SOURCE_MAP_KEY):
    file_regex_source_map = json.loads(args[Constant.HDFS_FILE_SOURCE_MAP_KEY])
  else:
    file_regex_source_map = None
  t.file_regex_source_map = file_regex_source_map
  t.transform(args[Constant.HDFS_SCHEMA_LOCAL_PATH_KEY], args[Constant.HDFS_SCHEMA_RESULT_KEY],
              args[Constant.HDFS_FIELD_RESULT_KEY])


