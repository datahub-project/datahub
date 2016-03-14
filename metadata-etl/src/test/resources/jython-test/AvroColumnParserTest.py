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

__author__ = 'zsun'
import unittest
import json
from AvroColumnParser import AvroColumnParser

sample_avro_with_complex_field ='''
  {
    "doc": "sample avro array",
    "fields": [{
      "doc": "doc1",
      "name": "emailAddresses",
      "type": {
        "items": {
          "doc": "Stores details about an email address that a user has associated with their account.",
          "fields": [{
            "doc": "The email address, e.g. `foo@example.com`",
            "name": "address",
            "type": "string"
          }, {
            "default": false,
            "doc": "true if the user has clicked the link in a confirmation email to this address.",
            "name": "verified",
            "type": "boolean"
          }, {
            "name": "dateAdded",
            "type": "long"
          }, {
            "name": "dateBounced",
            "type": ["null", "long"]
          }, {
            "name": "nestedField",
            "type": {
              "type":"record",
              "items": {
                "fields": [{
                "name": "nested1",
                "type": "string"
                }]
              }
            }
          }],
          "name": "EmailAddress",
          "type": "record"
        },
        "type": "array"
      }
    },
    {
  		"doc": "this is a enum type",
  		"type": "enum",
  		"name": "Suit",
  		"symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]

  	}
    ],
    "name": "User",
    "namespace": "com.example.avro",
    "type": "record",
    "uri": "hdfs:///data/sample1"
  }
  '''
# output value : List[List[]] : uri, sort_id, parent_sort_id, prefix, column_name, data_type, is_nullable, default_value, data_size, namespace, description
expect_result = [[u'hdfs:///data/sample1', 1, 0, '', u'emailAddresses', u'array', 'N', '', None, '', u'doc1'],
 [u'hdfs:///data/sample1', 2, 1, u'emailAddresses', u'address', u'string', 'N', '', None, '', u'The email address, e.g. `foo@example.com`'],
 [u'hdfs:///data/sample1', 3, 1, u'emailAddresses', u'verified', u'boolean', 'N', False, None, '', u'true if the user has clicked the link in a confirmation email to this address.'],
 [u'hdfs:///data/sample1', 4, 1, u'emailAddresses', u'dateAdded', u'long', 'N', '', None, '', ''],
 [u'hdfs:///data/sample1', 5, 1, u'emailAddresses', u'dateBounced', u'long', 'Y', '', None, '', ''],
 [u'hdfs:///data/sample1', 6, 1, u'emailAddresses', u'nestedField', u'record', 'N', '', None, '', ''],
 [u'hdfs:///data/sample1', 7, 6, u'emailAddresses.nestedField', u'nested1', u'string', 'N', '', None, '', ''],
 [u'hdfs:///data/sample1', 8, 0, '', u'Suit', u'enum', 'N', '', None, '', u'this is a enum type']]


class AvroColumnParserTest(unittest.TestCase):

  def test_parse_complex(self):
    avro_json = json.loads(sample_avro_with_complex_field)
    acp = AvroColumnParser(avro_json)
    result = acp.get_column_list_result()
    self.assertEqual(result, expect_result)


  def runTest(self):
    pass


if __name__ == '__main__':

  a = AvroColumnParserTest()
  a.test_parse_complex()

