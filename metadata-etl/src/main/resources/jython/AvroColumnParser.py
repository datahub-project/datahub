#!/usr/bin/env python
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

class AvroColumnParser:
  """
  This class is used to parse the avro schema, get a list of columns inside it.
  As avro is nested, we use a recursive way to parse it.
  Currently used in HDFS avro file schema parsing and Hive avro schema parsing.
  """

  def __init__(self, avro_schema, urn = None):
    """
    :param avro_schema: json of schema
    :param urn: optional, could contain inside schema
    :return:
    """
    self.sort_id = 0
    if not urn:
      self.urn = avro_schema['uri'] if 'uri' in avro_schema else None
    else:
      self.urn = urn
    self.result = []
    self.fields_json_to_csv(self.result, '', avro_schema['fields'])

  def get_column_list_result(self):
    """

    :return:
    """
    return self.result

  def fields_json_to_csv(self, output_list_, parent_field_path, field_list_):
    """
        Recursive function, extract nested fields out of avro.
    """
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
        [self.urn, self.sort_id, parent_id, parent_field_path, o_field_name, o_field_data_type, o_field_nullable,
         o_field_default, o_field_data_size, o_field_namespace,
         o_field_doc.replace("\n", ' ') if o_field_doc is not None else None])

      # check if this field is a nested record
      if type(f['type']) == dict and f['type'].has_key('fields'):
        current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
        self.fields_json_to_csv(output_list_, current_field_path, f['type']['fields'])
      elif type(f['type']) == dict and f['type'].has_key('items') and type(f['type']['items']) == dict and f['type']['items'].has_key('fields'):
        current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
        self.fields_json_to_csv(output_list_, current_field_path, f['type']['items']['fields'])

      if effective_type_index_in_type >= 0 and type(f['type'][effective_type_index_in_type]) == dict:
        if f['type'][effective_type_index_in_type].has_key('items') and type(
            f['type'][effective_type_index_in_type]['items']) == list:

          for item in f['type'][effective_type_index_in_type]['items']:
            if type(item) == dict and item.has_key('fields'):
              current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
              self.fields_json_to_csv(output_list_, current_field_path, item['fields'])
        elif f['type'][effective_type_index_in_type].has_key('items') and type(f['type'][effective_type_index_in_type]['items'])== dict and f['type'][effective_type_index_in_type]['items'].has_key('fields'):
          # type: [ null, { type: array, items: { name: xxx, type: record, fields: [] } } ]
          current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
          self.fields_json_to_csv(output_list_, current_field_path, f['type'][effective_type_index_in_type]['items']['fields'])
        elif f['type'][effective_type_index_in_type].has_key('fields'):
          # if f['type'][effective_type_index_in_type].has_key('namespace'):
          # o_field_namespace = f['type'][effective_type_index_in_type]['namespace']
          current_field_path = o_field_name if parent_field_path == '' else parent_field_path + '.' + o_field_name
          self.fields_json_to_csv(output_list_, current_field_path, f['type'][effective_type_index_in_type]['fields'])

          # End of function

