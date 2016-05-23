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

import unittest
import json
from HiveColumnParser import HiveColumnParser
import pprint

complex_type_kw = ['array', 'map', 'struct', 'uniontype']

sample_hive_schema ='''

  '''

sample_hive_schema_complex_types = "Complex Types"

expect_result = []

sample_input_simple = '{"fields" : [{"Comment": "document of field test", "TypeName": "struct<resultid:decimal(1,2),result:string>", "ColumnName": "test"}]}'

# output value : List[List[]] : uri, sort_id, parent_sort_id, prefix, column_name, data_type, is_nullable, default_value, data_size, namespace, description

expect_result_simple = [['hdfs:///test/urn', 1, 0, '', u'test', u'struct', None, None, None, None, u'document of field test'],
                 ['hdfs:///test/urn', 2, 1, u'test', u'resultid', u'decimal(1,2)', None, None, None, None, None],
                 ['hdfs:///test/urn', 3, 1, u'test', u'result', u'string', None, None, None, None, None]]


sample_input_complex = '''
{"fields" : [{"Comment": null, "TypeName": "struct<advancedfields:map<string,string>,\
facetvaluemap:map<string,string> comment 'MFC-J475',\
searchcomponents:array<struct<componenttype:string comment 'Work Smart Series',\
position:string,results:struct<numsearchresults:decimal(15,2),results:array<struct\
<resultid:bigint,result:varchar(10),resulttype:string,resultindex:int,relevance:float,\
additionalinfo:map<string,string>>>> comment 'com.brother.innobella.printer',\
additionalinfo:map<string,string>>>,searchtime:int comment '1~1024',\
extratag:uniontype<int,double,struct<aaa:int,bbb:char>,array<varchar>> comment '*',\
querytagger:string>", "ColumnName": "testcomplex"},{"Comment":null,"TypeName":"string","ColumnName":"extracolumn"}]}
'''

expect_result_complex = [['hdfs:///test/urn', 1, 0, '', u'testcomplex', u'struct', None, None, None, None, None],
['hdfs:///test/urn', 2, 1, u'testcomplex', u'advancedfields', u'map', None, None, None, None, None],
['hdfs:///test/urn', 3, 1, u'testcomplex', u'facetvaluemap', u'map', None, None, None, None, u'MFC-J475'],
['hdfs:///test/urn', 4, 1, u'testcomplex', u'searchcomponents', u'array', None, None, None, None, None],
['hdfs:///test/urn', 5, 4, u'testcomplex.searchcomponents', u'componenttype', u'string', None, None, None, None, u'Work Smart Series'],
['hdfs:///test/urn', 6, 4, u'testcomplex.searchcomponents', u'position', u'string', None, None, None, None, None],
['hdfs:///test/urn', 7, 4, u'testcomplex.searchcomponents', u'results', u'struct', None, None, None, None, u'com.brother.innobella.printer'],
['hdfs:///test/urn', 8, 7, u'testcomplex.searchcomponents.results', u'numsearchresults', u'decimal(15,2)', None, None, None, None, None],
['hdfs:///test/urn', 9, 7, u'testcomplex.searchcomponents.results', u'results', u'array', None, None, None, None, None],
['hdfs:///test/urn', 10, 9, u'testcomplex.searchcomponents.results.results', u'resultid', u'bigint', None, None, None, None, None],
['hdfs:///test/urn', 11, 9, u'testcomplex.searchcomponents.results.results', u'result', u'varchar(10)', None, None, None, None, None],
['hdfs:///test/urn', 12, 9, u'testcomplex.searchcomponents.results.results', u'resulttype', u'string', None, None, None, None, None],
['hdfs:///test/urn', 13, 9, u'testcomplex.searchcomponents.results.results', u'resultindex', u'int', None, None, None, None, None],
['hdfs:///test/urn', 14, 9, u'testcomplex.searchcomponents.results.results', u'relevance', u'float', None, None, None, None, None],
['hdfs:///test/urn', 15, 9, u'testcomplex.searchcomponents.results.results', u'additionalinfo', u'map', None, None, None, None, None],
['hdfs:///test/urn', 16, 4, u'testcomplex.searchcomponents', u'additionalinfo', u'map', None, None, None, None, None],
['hdfs:///test/urn', 17, 1, u'testcomplex', u'searchtime', u'int', None, None, None, None, u'1~1024'],
['hdfs:///test/urn', 18, 1, u'testcomplex', u'extratag', u'uniontype', None, None, None, None, u'*'],
['hdfs:///test/urn', 19, 18, u'testcomplex.extratag', 'type0', u'int', None, None, None, None, None],
['hdfs:///test/urn', 20, 18, u'testcomplex.extratag', 'type1', u'double', None, None, None, None, None],
['hdfs:///test/urn', 21, 18, u'testcomplex.extratag', 'type2', u'struct', None, None, None, None, None],
['hdfs:///test/urn', 22, 21, u'testcomplex.extratag.type2', u'aaa', u'int', None, None, None, None, None],
['hdfs:///test/urn', 23, 21, u'testcomplex.extratag.type2', u'bbb', u'char', None, None, None, None, None],
['hdfs:///test/urn', 24, 18, u'testcomplex.extratag', 'type3', u'array', None, None, None, None, None],
['hdfs:///test/urn', 25, 1, u'testcomplex', u'querytagger', u'string', None, None, None, None, None],
['hdfs:///test/urn', 26, 0, u'', u'extracolumn', u'string', None, None, None, None, None]]
class HiveColumnParserTest(unittest.TestCase):

  def test_parse_simple(self):
    schema = json.loads(sample_input_simple)
    hcp = HiveColumnParser(schema, urn = 'hdfs:///test/urn')

    #pprint.pprint(hcp.column_type_dict)

    self.assertEqual(hcp.column_type_list, expect_result_simple)

  def test_parse_complex(self):

    schema = json.loads(sample_input_complex)
    hcp = HiveColumnParser(schema, urn = 'hdfs:///test/urn')

    #pprint.pprint(hcp.column_type_dict)

    self.assertEqual(hcp.column_type_list,expect_result_complex)

  def runTest(self):
    pass


if __name__ == '__main__':

  a = HiveColumnParserTest()
  a.test_parse_simple()
  a.test_parse_complex()


