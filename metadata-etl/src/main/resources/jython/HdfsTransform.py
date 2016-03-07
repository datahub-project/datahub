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

    def fields_json_to_csv(output_list_, parent_field_path, field_list_):
      # string, list, int, optional int
      self.sort_id
      parent_field_path
      parent_id = self.sort_id

      for f in field_list_:
        self.sort_id += 1

        o_field_name = f['name']
        o_field_data_type = ''
        o_field_data_size = None
        o_field_nullable = 'N'
        o_field_default = ''
        o_field_namespace = ''
        o_field_doc = ''
        effective_type_index_in_type = -1

        if f.has_key('namespace'):
          o_field_namespace = f['namespace']

        if f.has_key('default') and type(f['default']) != None:
          o_field_default = f['default']

        if not f.has_key('type'):
          o_field_data_type = None
        elif type(f['type']) == list:
          i = effective_type_index = -1
          for data_type in f['type']:
            i += 1  # current index
            if type(data_type) is None or (data_type == 'null'):
              o_field_nullable = 'Y'
            elif type(data_type) == dict:
              o_field_data_type = data_type['type']
              effective_type_index_in_type = i

              if data_type.has_key('namespace'):
                o_field_namespace = data_type['namespace']
              elif data_type.has_key('name'):
                o_field_namespace = data_type['name']

              if data_type.has_key('size'):
                o_field_data_size = data_type['size']
              else:
                o_field_data_size = None

            else:
              o_field_data_type = data_type
              effective_type_index_in_type = i
        elif type(f['type']) == dict:
          o_field_data_type = f['type']['type']
        else:
          o_field_data_type = f['type']
          if f.has_key('attributes') and f['attributes'].has_key('nullable'):
            o_field_nullable = 'Y' if f['attributes']['nullable'] else 'N'
          if f.has_key('attributes') and f['attributes'].has_key('size'):
            o_field_data_size = f['attributes']['size']

        if f.has_key('doc'):
          if len(f['doc']) == 0 and f.has_key('attributes'):
            o_field_doc = json.dumps(f['attributes'])
          else:
            o_field_doc = f['doc']
        elif f.has_key('comment'):
          o_field_doc = f['comment']

        output_list_.append(
          [o_urn, self.sort_id, parent_id, parent_field_path, o_field_name, o_field_data_type, o_field_nullable,
           o_field_default, o_field_data_size, o_field_namespace,
           o_field_doc.replace("\n", ' ') if o_field_doc is not None else None])

        # check if this field is a nested record
        if type(f['type']) == dict and f['type'].has_key('fields'):
          current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
          fields_json_to_csv(output_list_, current_field_path, f['type']['fields'])
        elif type(f['type']) == dict and f['type'].has_key('items') and type(f['type']['items']) == dict and f['type']['items'].has_key('fields'):
          current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
          fields_json_to_csv(output_list_, current_field_path, f['type']['items']['fields'])

        if effective_type_index_in_type >= 0 and type(f['type'][effective_type_index_in_type]) == dict:
          if f['type'][effective_type_index_in_type].has_key('items') and type(
              f['type'][effective_type_index_in_type]['items']) == list:

            for item in f['type'][effective_type_index_in_type]['items']:
              if type(item) == dict and item.has_key('fields'):
                current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
                fields_json_to_csv(output_list_, current_field_path, item['fields'])
          elif f['type'][effective_type_index_in_type].has_key('items') and f['type'][effective_type_index_in_type]['items'].has_key('fields'):
            # type: [ null, { type: array, items: { name: xxx, type: record, fields: [] } } ]
            current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
            fields_json_to_csv(output_list_, current_field_path, f['type'][effective_type_index_in_type]['items']['fields'])
          elif f['type'][effective_type_index_in_type].has_key('fields'):
            # if f['type'][effective_type_index_in_type].has_key('namespace'):
            # o_field_namespace = f['type'][effective_type_index_in_type]['namespace']
            current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
            fields_json_to_csv(output_list_, current_field_path, f['type'][effective_type_index_in_type]['fields'])

            # End of function

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

        fields_json_to_csv(o_field_list_, '', j['fields'])

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


