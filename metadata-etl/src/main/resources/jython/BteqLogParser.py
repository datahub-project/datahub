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
from metadata.etl.sqlParser import SqlParser
from pyparsing import *
import ParseUtil
import ConfigParser, os
import DbUtil
import sys
import gzip
import StringIO
import json
import datetime
import time
import re
from org.slf4j import LoggerFactory

class BteqLogParser:

  def __init__(self, file_content=None, log_file_name=None):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.extractCount = 0
    merge_kw = Keyword('MERGE',caseless=True)
    delete_kw = Keyword('DELETE',caseless=True) | Keyword('DEL',caseless=True)
    select_kw = Keyword('SELECT',caseless=True) | Keyword('SEL',caseless=True)
    update_kw = Keyword('UPDATE',caseless=True) | Keyword('UPD',caseless=True)
    insert_kw = Keyword('INSERT',caseless=True) | Keyword('INS',caseless=True)
    create_kw = Keyword('CREATE',caseless=True) | Keyword('CRE',caseless=True)

    nested_expr = nestedExpr() | nestedExpr('{','}')


    insert_stmt = originalTextFor(insert_kw + SkipTo(Literal(';')|StringEnd(),ignore = nested_expr|quotedString))
    create_stmt = originalTextFor(create_kw + SkipTo(Literal(';')|StringEnd(),ignore = nested_expr|quotedString))
    update_stmt = originalTextFor(update_kw + SkipTo(Literal(';')|StringEnd(),ignore = nested_expr|quotedString))
    delete_stmt = originalTextFor(delete_kw + SkipTo(Literal(';')|StringEnd(),ignore = nested_expr|quotedString))
    merge_stmt = originalTextFor(merge_kw + SkipTo(Literal(';')|StringEnd(),ignore = nested_expr|quotedString))
    select_stmt = originalTextFor(select_kw + SkipTo(Literal(';')|StringEnd(),ignore = nested_expr|quotedString))

    statements = insert_stmt | create_stmt | update_stmt | delete_stmt | merge_stmt |select_stmt

    '''%%%%%%%%%%%%%%%% define to fix the "time" parsing bugs %%%%%%%%%%%%%%%%%%%%%%%%'''
    '''exmaple : cast(:date_sk as date format 'yyyymmdd'),'''
    ignore_cast = Keyword("CAST", caseless=True) + Literal('(') + SkipTo(Literal(')'),
                                                                         ignore=nested_expr | quotedString) + Literal(')') + Optional(Keyword("at", caseless=True) + Literal('\'') + SkipTo(Literal('\''))
                                                                                                                                      + Literal('\''));
    ignore_cast_extend = Literal('(') + ignore_cast + SkipTo(Literal(')'),
                                                             ignore=nested_expr | quotedString) + Literal(')');

    #no need to know what's in the create table content
    ident = (Word(alphas + "$", alphanums + "_.::$/") | Word('$', nums))
    self.find_create = CaselessLiteral("create ")+ Optional(CaselessLiteral('volatile '))+CaselessLiteral('table ') + ident +Literal('(') + SkipTo(Literal(')'),ignore = nested_expr|quotedString).setParseAction(replaceWith("aa int "))  + Literal(')');

    self.ignore_stat = ignore_cast_extend | ignore_cast;
    ignore_not_found_begin = Literal('.LABEL NO_DATA_FOUND')
    ignore_not_found_end = Literal('.QUIT 1') | Literal('.EXIT 1')
    ignore_not_found = ignore_not_found_begin + SkipTo(ignore_not_found_end) + ignore_not_found_end

    #ignore comments and remarks
    ignore_comment = cStyleComment | '--'+restOfLine | CaselessKeyword('.REMARK')+restOfLine

    #ignore select current_date
    ignore_sel_current_date = (CaselessLiteral('select') | CaselessLiteral('sel')) + CaselessLiteral('current_date') + Literal(';')

    self.file_parser = (statements).ignore(ignore_comment | ignore_not_found | ignore_sel_current_date);

  def analyze_nested_source(self, nested_source, transformation):
    """ the source of a statement can be nested,
    extract all source tables in a recursive function """
    for nested_nested_source in nested_source['nestedSource']:
      self.analyze_nested_source(nested_nested_source, transformation)

    for source in nested_source['sourceTables']:
      transformation['source_relations'].append(source)

  def convert_to_flat_dict_recursive(self, result, transformations):
    """ recursive call to transform nested structure into flat structure """
    transformation = {}
    transformation.setdefault('source_relations',[])
    transformation.setdefault('target_relations',[])
    transformation['operation'] = result['Op']

    for nested_source in result['nestedSource']:
      self.analyze_nested_source(nested_source,transformation)

    for source in result['sourceTables']:
      transformation['source_relations'].append(source)
    for target in result['targetTables']:
      transformation['target_relations'].append(target)

    transformation['start_line'] = result['start_line']
    transformation['end_line'] = result['end_line']

    if result['Op'] == 'CREATE':
      transformation['isVolatile'] = result['isVolatile']

    if result.has_key('whereClause'):
      transformation['whereClause'] = result['whereClause']

    transformations.append(transformation)

  def convert_to_transformations(self, resultList):
    """ The output of java file is a nested structure
        need to convert to a falt in a recursive call"""
    transformations = []

    for result in resultList:
      self.convert_to_flat_dict_recursive(result, transformations)

    return transformations

  def parse(self, key_values, file_name, script_path, script_name, bteq_source_target_override_file, metric_override_file):
    metric_sk = Word(nums)
    metric_sk_list = delimitedList(metric_sk)
    in_list_condition = CaselessKeyword('in').suppress() + Literal('(').suppress() + \
                        metric_sk_list + Literal(')').suppress()
    equality_condition = Literal('=').suppress() + metric_sk
    between_condition = CaselessKeyword('between') + metric_sk + \
                        CaselessKeyword('AND').suppress() + metric_sk
    metric_sk_condition = CaselessKeyword('metric_sk').suppress() + \
                          (equality_condition | in_list_condition | between_condition)
    entries = []

    try:
      transformations = self.parseSql(file_name)
    except Exception as e:
      self.logger.warn("Fails to parse %s %s" % (script_path, script_name))
      self.logger.warn("with error {0}".format(e))
      cp = ConfigParser.ConfigParser()
      cp.read(bteq_source_target_override_file)
      cfg = {}
      try:
        for k,v in cp.items('%s/%s' % (script_path, script_name)):
          if k not in ['target_relations', 'source_relations']:
            cfg[k] = v
          else:
            cfg[k] = v.split(',')
        transformations.append(cfg)
        self.logger.info("Completed parsing script: %s/%s using override" %
                         (script_path, script_name))
      except ConfigParser.NoSectionError:
        self.logger.error("No override found in %s" % bteq_source_target_override_file)
        return entries

    metric_override = eval(open(metric_override_file).read())

    # Perform any parameter substitution that is needed
    for t in transformations:
      for index, src in enumerate(t['source_relations']):
        t['source_relations'][index] = t['source_relations'][index].upper()
        if src.startswith('$') and key_values.has_key(src[1:]):
          t['source_relations'][index] = key_values[src[1:]].upper()
      for index, tgt in enumerate(t['target_relations']):
        t['target_relations'][index] = t['target_relations'][index].upper()
        if tgt.startswith('$') and key_values.has_key(tgt[1:]):
          t['target_relations'][index] = key_values[tgt[1:]].upper()
      if t['operation'].upper() in ['DEL','DELETE'] and t.has_key('whereClause'):
        for tokens, start, stop in metric_sk_condition.scanString(t['whereClause'].encode('latin-1')):
          parsed_metric = tokens.asList()
          if parsed_metric[0].upper() == 'BETWEEN':
            if not t.has_key('metric_sk'):
              t['metric_sk'] = []
            t['metric_sk'].extend(range(int(parsed_metric[1]),int(parsed_metric[2]) + 1))
          else:
            if not t.has_key('metric_sk'):
              t['metric_sk'] = []
            t['metric_sk'].extend(map(int,parsed_metric))

    # Collect all volatile table names. They shouldn't appear as source / target
    # in top level lineage diagram
    volatile_table = {}
    for t in transformations:
      for target in t['target_relations']:
        if t.has_key('isVolatile'):
          volatile_table[target] = True

    processed_table = {}

    for t in transformations:
      if t['type'] != 'explicit' or t['operation'] == 'DUMP': continue
      for src in t['source_relations']:
        source_schema_name, source_table_name = ParseUtil.get_db_table_abstraction(src)
        full_table_name = source_schema_name + '.' + source_table_name
        if not volatile_table.has_key(src):
          if not processed_table.has_key(full_table_name):
            if src.encode('latin-1').startswith('P_'): continue
            entries.append(
              {'script_file' : script_name,
               'script_path' : script_path,
               'script_type' : 'SQL',
               'relation' : source_table_name,
               'schema_name' : source_schema_name,
               'table_type' : 'source',
               'start_line' : t['start_line'],
               'object_type' : 'Table',
               'storage_type' : 'Teradata',
               'operation' : 'SELECT'})
            processed_table[full_table_name] = len(entries) - 1

    processed_table = {}
    for t in transformations:
      if t['type'] != 'explicit' or t['operation'] == 'DUMP': continue
      for tgt in t['target_relations']:
        target_schema_name, target_table_name = ParseUtil.get_db_table_abstraction(tgt)
        full_table_name = target_schema_name + '.' + target_table_name
        if not volatile_table.has_key(tgt):
          if not processed_table.has_key(full_table_name) :
            if tgt.encode('latin-1').startswith('P_'): continue
            entries.append({
              'script_file' : script_name,
              'script_path' : script_path,
              'script_type' : 'SQL',
              'relation' : target_table_name,
              'schema_name' : target_schema_name,
              'start_line' : t['start_line'],
              'operation' : t['operation'].encode('latin-1'),
              'object_type' : 'Table',
              'storage_type' : 'Teradata',
              'table_type' : 'target'})
            if t.has_key('metric_sk'):
              entries[len(entries) - 1]['metric_sk'] = []
              for m in t['metric_sk']:
                if m not in entries[len(entries) - 1]['metric_sk']:
                  entries[len(entries) - 1]['metric_sk'].append(m)
            else:
              entries[len(entries) - 1]['metric_sk'] = []
              try:
                for m in metric_override[os.path.join(
                        script_path, script_name)][target_table_name.lower()]:
                  entries[len(entries) - 1]['metric_sk'].append(m)
              except KeyError:
                pass
            processed_table[full_table_name] = len(entries) - 1
          else:
            if t['operation'] not in entries[processed_table[full_table_name]]['operation'].split(','):
              entries[processed_table[full_table_name]]['operation'] = entries[processed_table[full_table_name]]['operation'] + ',' + t['operation']
            if t.has_key('metric_sk'):
              if not entries[processed_table[full_table_name]].has_key('metric_sk'):
                entries[len(entries) - 1]['metric_sk'] = []
              for m in t['metric_sk']:
                if m not in entries[processed_table[full_table_name]]['metric_sk']:
                  entries[processed_table[full_table_name]]['metric_sk'].append(m)
    return entries

  def parseSql(self, file_name):

    if file_name is not None:
      file_name = os.path.abspath(file_name)
      bteq_file = open(file_name, 'r')

      text = bteq_file.read()
      text = text.replace('',r'\\u001A')
      bteq_file.close()
    else:
      text = sys.stdin.read()

    self.file_parser.parseWithTabs()
    all_statements = self.file_parser.scanString(text)
    transformations = []  # array to hold transformation definitions

    resultList = []

    for tokens,startloc,endloc in all_statements:
      global parent_start_line,parent_end_line
      parent_start_line = lineno(startloc,text)
      parent_end_line = lineno(endloc,text)

      #replace the string that we can not parse
      '''%%%%%%%%%%%%%%%% to fix the "time" parsing bugs %%%%%%%%%%%%%%%%%%%%%%%%'''
      self.ignore_stat.setParseAction(replaceWith("(0)"))
      s = tokens[0].lower()
      s = self.ignore_stat.transformString(s)
      #print s
      s = self.find_create.transformString(s)
      insert_values = 'when not matched then' + Literal('insert').setParseAction(
        replaceWith(' insert values')) + '(' + SkipTo(')') + ')' + ~Literal('value')
      s = insert_values.transformString(s)
      unique_replace = 'count' + '(' + Literal('unique').setParseAction(replaceWith(''))
      s = unique_replace.transformString(s)
      #TODO fix multiset & recursive keyword bugs
      #replace_str = re.compile('set (?=table)|multiset|recursive', re.I)
      #s = replace_str.sub('', s)
      #s = re.sub(r"(?i)create set volatile", "create volatile",s)
      #print s

      # temporary fix
      s = s.replace('(date)', '')

      """
      check_output in subprocess module is only available from 2.7
      and upwards.  To make code campatible with python 2.6 I switched
      check_output with Popen
      """
      output = SqlParser.parse(s)
      if output:
        result = json.loads(output)
        result['start_line'] = parent_start_line
        result['end_line'] = parent_end_line
        resultList.append(result)

    transformations = self.convert_to_transformations(resultList)
    for t in transformations:
      #t['source_relations'] = map(lambda s:self.__deCompoundName(s), t['source_relations'])
      ''' remove duplicate element '''
      t['source_relations'] = list(set(t['source_relations']))

      #t['target_relations'] = map(lambda s:self.__deCompoundName(s), t['target_relations'])
      t['target_relations'] = list(set(t['target_relations']))

      if t['operation'] == 'SELECT' :
        t['target_relations'].append('dumpData_'+str(self.extractCount))
        self.extractCount += 1
      if t['source_relations'] == [] and t['operation'] == 'SELECT':
        t['type'] = 'hidden'
      #elif t['source_relations'] == [] and t['operation'] == 'CREATE':
      # t['type'] = 'hidden'
      else:
        t['type'] = 'explicit'

      if t['operation'] == 'SELECT':
        t['operation'] = 'DUMP'

    return transformations

if __name__ == "__main__":
  props = sys.argv[1]
  bteq_parser = BteqLogParser()
