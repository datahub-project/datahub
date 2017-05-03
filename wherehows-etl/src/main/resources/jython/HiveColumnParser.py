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


import re
import json


class HiveColumnParser:
  """
  This class used for parsing a Hive metastore schema.
  Major effort is parse complex column types :
  Reference : https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
  """
  def string_interface(self, dataset_urn, input):
    schema = json.loads(input)
    return HiveColumnParser(dataset_urn, schema)

  def __init__(self, avro_schema, urn = None):
    """

    :param avro_schema json schema
    :param urn:
    :return:
    """
    self.prefix = ''
    if not urn:
      self.dataset_urn = avro_schema['uri'] if 'uri' in avro_schema else None
    else:
      self.dataset_urn = urn
    self.column_type_dict = avro_schema
    self.column_type_list = []
    self.sort_id = 0
    for index, f in enumerate(avro_schema['fields']):
      avro_schema['fields'][index] = self.parse_column(f['ColumnName'], f['TypeName'], f['Comment'])

    # padding list for rest 4 None, skip  is_nullable, default_value, data_size, namespace for now
    self.column_type_list = [(x[:6] + [None] * 4 + x[6:]) for x in self.column_type_list]

  def parse_column(self, column_name, type_string, comment=None):
    """
    Returns a dictionary of a Hive column's type metadata and
     any complex or nested type info
    """
    simple_type, inner, comment_inside = self._parse_type(type_string)
    comment = comment if comment else comment_inside
    column = {'name': column_name, 'type' : simple_type, 'doc': comment}

    self.sort_id += 1
    self.column_type_list.append([self.dataset_urn, self.sort_id, 0, self.prefix, column_name, simple_type, comment])
    if inner:
      self.prefix = column_name
      column.update(self._parse_complex(simple_type, inner, self.sort_id))

    # reset prefix after each outermost field
    self.prefix = ''
    return column

  def is_scalar_type(self, type_string):
    return not (type_string.startswith('array') or type_string.startswith('map') or type_string.startswith(
      'struct') or type_string.startswith('uniontype'))

  def _parse_type(self, type_string):
    pattern = re.compile(r"^([a-z]+[(),0-9]*)(<(.+)>)?( comment '(.*)')?$", re.IGNORECASE)
    match = re.search(pattern, type_string)
    if match is None:
      return None, None, None
    return match.group(1), match.group(3), match.group(5)

  def _parse_complex(self, simple_type, inner, parent_id):
    complex_type = {}
    if simple_type == "array":
      complex_type['item'] = self._parse_array_item(inner, parent_id)
    elif simple_type == "map":
      complex_type['key'] = self._parse_map_key(inner)
      complex_type['value'] = self._parse_map_value(inner, parent_id)
    elif simple_type == "struct":
      complex_type['fields'] = self._parse_struct_fields(inner, parent_id)
    elif simple_type == "uniontype":
      complex_type['union'] = self._parse_union_types(inner, parent_id)
    return complex_type

  def _parse_array_item(self, inner, parent_id):
    item = {}
    simple_type, inner, comment = self._parse_type(inner)
    item['type'] = simple_type
    item['doc'] = comment
    if inner:
      item.update(self._parse_complex(simple_type, inner, parent_id))
    return item

  def _parse_map_key(self, inner):
    key = {}
    key_type = inner.split(',', 1)[0]
    key['type'] = key_type
    return key

  def _parse_map_value(self, inner, parent_id):
    value = {}
    value_type = inner.split(',', 1)[1]
    simple_type, inner, comment = self._parse_type(value_type)
    value['type'] = simple_type
    value['doc'] = comment
    if inner:
      value.update(self._parse_complex(simple_type, inner, parent_id))
    return value

  def _parse_struct_fields(self, inner, parent_id):
    current_prefix = self.prefix
    fields = []
    field_tuples = self._split_struct_fields(inner)
    for (name, value) in field_tuples:
      field = {}
      name = name.strip(',')
      field['name'] = name
      simple_type, inner, comment = self._parse_type(value)
      field['type'] = simple_type
      field['doc'] = comment
      self.sort_id += 1
      self.column_type_list.append([self.dataset_urn, self.sort_id, parent_id, self.prefix, name, simple_type, comment])
      if inner:
        self.prefix += '.' + name
        field.update(self._parse_complex(simple_type, inner, self.sort_id))
        self.prefix = current_prefix
      fields.append(field)
    return fields

  def _split_struct_fields(self, fields_string):
    fields = []
    remaining = fields_string
    while remaining:
      (fieldname, fieldvalue), remaining = self._get_next_struct_field(remaining)
      fields.append((fieldname, fieldvalue))
    return fields

  def _get_next_struct_field(self, fields_string):
    fieldname, rest = fields_string.split(':', 1)
    balanced = 0
    for pos, char in enumerate(rest):
      balanced += {'<': 1, '>': -1, '(': 100, ')': -100}.get(char, 0)
      if balanced == 0 and char in ['>', ',']:
        if rest[pos + 1:].startswith(' comment '):
          pattern = re.compile(r"( comment '.*?')(,[a-z_0-9]+:)?", re.IGNORECASE)
          match = re.search(pattern, rest[pos + 1:])
          cm_len = len(match.group(1))
          return (fieldname, rest[:pos + 1 + cm_len].strip(',')), rest[pos + 2 + cm_len:]
        return (fieldname, rest[:pos + 1].strip(',')), rest[pos + 1:]
    return (fieldname, rest), None

  def _parse_union_types(self, inner, parent_id):
    current_prefix = self.prefix
    types = []
    type_tuples = []
    bracket_level = 0
    current = []

    for c in (inner + ","):
      if c == "," and bracket_level == 0:
        type_tuples.append("".join(current))
        current = []
      else:
        if c == "<":
          bracket_level += 1
        elif c == ">":
          bracket_level -= 1
        current.append(c)


    for pos, value in enumerate(type_tuples):
      field = {}
      name = 'type' + str(pos)
      field['name'] = name
      #print 'type' + str(pos) + "\t" + value
      simple_type, inner, comment = self._parse_type(value)
      field['type'] = simple_type
      field['doc'] = comment
      self.sort_id += 1
      self.column_type_list.append([self.dataset_urn, self.sort_id, parent_id, self.prefix, name, simple_type, comment])

      if inner:
        self.prefix += '.' + name
        field.update(self._parse_complex(simple_type, inner, self.sort_id))
        self.prefix = current_prefix
      types.append(field)
    return types
