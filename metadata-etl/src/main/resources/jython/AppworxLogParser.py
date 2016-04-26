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
import ast
import DbUtil
import sys
import gzip
import StringIO
import json
import datetime
import time
import re
from org.slf4j import LoggerFactory

ParserElement.enablePackrat()

class AppworxLogParser:

  def __init__(self, file_content=None, log_file_name=None):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.text = None

    if log_file_name is not None:
      self.log_file_name = log_file_name
      file_ext = os.path.splitext(self.log_file_name)[1][1:]
      self.logger.info(file_ext)
      self.logger.info(self.log_file_name)
      if file_ext == 'gz':
        try:
          self.text = gzip.GzipFile(mode='r', filename=self.log_file_name).read()
        except:
          self.logger.error('exception')
          self.logger.error(str(sys.exc_info()[0]))
      else:
        self.text = open(self.log_file_name,'r').read()
    else:
      self.text = file_content

  def match_one(self, grammar, text):
    try:
      match, start, end = grammar.scanString(text).next()
      return text[:start].strip(), match, text[end:].strip()
    except:
      return text, None, None

  def parse_log(self, flags, command_type=None):
    self.logger.info("parse_log")
    result = {}
    parameter_line = Keyword('parameter file').suppress() + \
                     OneOrMore(Word(printables,excludeChars='=') + \
                               Literal('=').suppress() + \
                               SkipTo(LineEnd()) + LineEnd().suppress()) + \
                     Keyword('End of parameter file').suppress()

    before_match, match, after_match = self.match_one(parameter_line, self.text)

    try:
      parameters = match.asList()
      self.logger.info(str(parameters))
    except:
      self.logger.error("Fails to find parameter section in file: %s" % self.log_file_name)
      self.logger.error(str(sys.exc_info()[0]))
      return {}
    param_dict = dict(zip(parameters[0::2], parameters[1::2]))
    result['job_parameters'] = param_dict

    script_name_line = Literal('GENERAL INFORMATION').suppress() + \
                       SkipTo(lineEnd).suppress() + \
                       SkipTo('program').suppress() + \
                       Literal('program').suppress() + \
                       SkipTo(lineEnd)
    merged_file_name = Word(printables, excludeChars=':')
    merged_file_line = Keyword('Starting merge of').suppress() + \
                       merged_file_name + Literal(':').suppress()
    processing_dir = Keyword('input folder is now').suppress() + \
                     Word(printables)

    tpt_source_file = CaselessKeyword('processing').suppress() + \
                      CaselessKeyword('file').suppress() + \
                      quotedString.setResultsName('source_file') + \
                      Literal('.').suppress() + \
                      LineEnd().suppress()
    tpt_target_table = CaselessKeyword('Target').suppress() + \
                       CaselessKeyword('Table').suppress() + \
                       Literal(':').suppress() + \
                       quotedString.setResultsName('target_table') + \
                       LineEnd().suppress()
    tpt_target_rows = CaselessKeyword('Total Rows Applied').suppress() + \
                     Literal(':').suppress() + Word(nums).setResultsName('insert_count') + \
                     LineEnd().suppress()
    tpt_insert_rows = CaselessKeyword('Rows Inserted').suppress() + \
                      Literal(':').suppress() + Word(nums).setResultsName('insert_count') + \
                      LineEnd().suppress()
    infa_run_id = Keyword('Workflow').suppress() + Word(printables).suppress() + \
                  Keyword('with').suppress() + \
                  SkipTo('run id').suppress() + Keyword('run id').suppress() + \
                  Literal('[').suppress() + Word(nums).setResultsName('run_id') + \
                  Literal(']').suppress() + Keyword('started')
    mr_job_id = r'job_[0-9]{13}_[0-9]+'
    mr_job_id_map = {}
    for match in re.findall(mr_job_id, self.text):
      if match not in mr_job_id_map:
        mr_job_id_map[match] = 1
    if len(mr_job_id_map):
      result['ref_mr_job_ids'] = mr_job_id_map.keys()

    if command_type in ['LI_TPT_LOADER', 'LI_TPT_INSERT']:
      tpt_source_file = Keyword('The input files are').suppress() + \
                        Literal('-').suppress() + \
                        Word(printables).setResultsName('source_file')

      tpt_target_table = CaselessKeyword('TargetTable').suppress() + \
                         Literal('=').suppress() + \
                         Word(printables).setResultsName('target_table') + \
                         LineEnd().suppress()
    if command_type is None:
      tpt_target_table.setParseAction(removeQuotes)
      tpt_source_file.setParseAction(removeQuotes)

    lparen = Literal('(').suppress()
    rparen = Literal(')').suppress()
    semicolon = Literal(';').suppress()

    import_kw = Keyword('.Import Infile').suppress()
    values_kw = CaselessKeyword('VALUES').suppress()
    into_kw = CaselessKeyword('INTO').suppress()
    insert_kw = CaselessKeyword('INSERT').suppress()
    statistics_kw = Keyword("Statistics for table").suppress()
    inserts_kw = Keyword("Inserts:")
    updates_kw = Keyword("Updates:")
    deletes_kw = Keyword("Deletes:")

    insert_sql = insert_kw + into_kw + \
                 Word(alphas, alphanums + '_').setResultsName('table_name') + \
                 lparen + \
                 SkipTo(rparen, ignore=quotedString | nestedExpr()).suppress() + \
                 rparen + \
                 values_kw + lparen + \
                 SkipTo(rparen, ignore=quotedString | nestedExpr()).suppress() + \
                 rparen + \
                 semicolon + Word(nums).suppress() + \
                 import_kw + \
                 SkipTo('Layout InputFileLayout').setResultsName('input_file') + \
                 SkipTo(statistics_kw).suppress() + SkipTo(LineEnd()).suppress() + \
                 SkipTo(inserts_kw).suppress() + inserts_kw.suppress() + Word(nums).setResultsName('insert_count') + \
                 OneOrMore((updates_kw | deletes_kw).suppress() + \
                           Word(nums)).setResultsName('record_count',listAllMatches=True)

    insert_sql.setParseAction(lambda t:[x.strip().replace('\n','').replace(' ','') for x in t])
    logon_kw = Keyword('*** Logon successfully completed.').suppress()
    credential_line = Keyword('.logon').suppress() + \
                      Combine(NotAny(Literal('$')) + Word(printables, excludeChars='/')).setResultsName('db_host') + \
                      Literal('/').suppress() + \
                      Combine(NotAny(Literal('$')) + Word(printables, excludeChars=',')).setResultsName('user_account') + \
                      Literal(',').suppress() + SkipTo(logon_kw) + \
                      FollowedBy(logon_kw).suppress()

    if command_type == 'LI_BTEQ':

        insert_kw = CaselessKeyword('INSERT')
        file_name = Word(printables,excludeChars='+')
        table_name = Word(alphas, alphanums + '_')
        dim_fact_agg_table_name = Combine(oneOf(['FACT_','DIM_','AGG_'], caseless=True) + Word(alphanums + '_'))
        file_kw = CaselessKeyword('FILE').suppress()
        other_words = OneOrMore(~insert_kw + Word(printables))
        import_line = CaselessKeyword('IMPORT').suppress() + \
                      CaselessKeyword('VARTEXT').suppress() + \
                      quotedString.suppress() + \
                      file_kw + Literal('=').suppress() + \
                      file_name + other_words.suppress() + \
                      insert_kw.suppress()  + \
                      CaselessKeyword('INTO').suppress() + \
                      table_name

        insert_stats_line = CaselessKeyword('INSERT').suppress() + CaselessKeyword('INTO').suppress() + \
                            dim_fact_agg_table_name.setResultsName('target_table_name') + \
                            SkipTo(CaselessKeyword('FROM') + CaselessLiteral('STG_'), include=False).suppress() + \
                            CaselessKeyword('FROM').suppress() + Word(alphanums + '_').setResultsName('source_table_name') + \
                            SkipTo(Literal('*** Insert completed.'), include=True).suppress() + \
                            Word(nums).setResultsName('insert_count') + \
                            Literal('rows added').suppress()

        script_name_line = (Keyword("run_sql").suppress() + \
                            Literal('[').suppress() + \
                            Word(printables, excludeChars=']').setResultsName('program_name') + \
                            Literal(']').suppress()) | Keyword('run_sql=').suppress() + \
                                                       Word(printables).setResultsName('program_name')

        dt_string = Word(nums,exact=4) + Literal('-') + \
                    Word(nums,exact=2) + Literal('-') + \
                    Word(nums,exact=2) + \
                    delimitedList(Word(nums,exact=2),':')  + \
                    Literal(',') + Word(nums)

        session_id = CaselessLiteral('select SESSION;').suppress() + \
                     SkipTo(Literal('Session'), include=True).suppress() + LineEnd().suppress() + \
                     Word(nums).setResultsName('session_id')

        db_kw = Keyword('DB variables') + Word(alphanums)

        pipe_char = Literal('|').suppress()

        db_vars = dt_string.suppress() + pipe_char + \
                  Word(alphanums + '.' + '_').suppress() + pipe_char + \
                  Word(nums).suppress() + pipe_char + \
                  Keyword('INFO').suppress() + pipe_char + \
                  db_kw.suppress() + Literal(':').suppress() + \
                  Literal('{').suppress() + SkipTo(Literal('}'),ignore=quotedString|nestedExpr('{','}'))

        before_match, match, after_match = self.match_one(db_vars, self.text)
        if match is not None:
            result['db_vars'] = []
            for k,v in ast.literal_eval('{' + match[0] + '}').items():
                k = k.strip()
                v = v.strip()
                if v[0] == "'":
                    v = v[1:-1]
                result['db_vars'].append({k : v})

        before_match, match, after_match = self.match_one(session_id, self.text)
        if match is not None:
            result['session_id'] = match[0]

        before_match, match, after_match = self.match_one(insert_stats_line, self.text)
        if match is not None:
            target_table_name = match['target_table_name']
            source_table_name = match['source_table_name']
            insert_count = match['insert_count']
            # print "%s: %s rows inserted. %s\n" % (table_name, insert_count, str(param_dict))
        else:
            source_table_name = None
            insert_count = 0

        before_match, match, after_match = self.match_one(script_name_line, self.text)
        if match is not None:
            result['script_name'] = os.path.basename(match[0])
            result['script_path'] = os.path.dirname(match[0])
            before_match, match, after_match = self.match_one(credential_line, self.text)
            if match is not None:
                result['host'] = match[0].strip().split('.')[0].upper()
                result['account'] = match[1]

            for tokens, start, stop in import_line.scanString(self.text):
                result['table'] = []
                result['table'].append(ParseUtil.get_nas_location_abstraction(tokens.asList()[0]))
                result['table'][len(result['table']) - 1].update({
                    'storage_type' : 'NAS',
                    'table_type' : 'source',
                    'full_path' : tokens.asList()[0]
                })
                schema_name, tab_name = ParseUtil.get_db_table_abstraction(tokens.asList()[1].upper())
                result['table'].append({
                    'account' : match[1],
                    'host' : match[0].strip().split('.')[0].upper(),
                    'storage_type' : 'Teradata',
                    'table_type' : 'target',
                    'table_name' : tab_name,
                    'schema_name' : schema_name,
                    'insert_count' : insert_count
                })

            if insert_count and not 'table' in result:
                result['table'] = [
                    {
                        'storage_type' : 'Teradata',
                        'table_type' : 'source',
                        'schema_name' : '',
                        'table_name' : source_table_name,
                        'abstracted_path' : source_table_name,
                        'full_path' : 'DWH_STG.' + source_table_name
                    }, {
                        'storage_type' : 'Teradata',
                        'table_type' : 'target',
                        'schema_name' : '',
                        'table_name' : target_table_name,
                        'abstracted_path' : target_table_name,
                        'full_path' : 'DWH_STG.' + target_table_name,
                        'insert_count' : insert_count
                    }]
            return result
        else:
            self.logger.warn("Fails to get script file name in %s" % self.log_file_name)
            return {}

    if flags.has_key('SCRIPT'):
      before_match, match, after_match = self.match_one(script_name_line, self.text)
      if match is not None:
        result['script_name'] = os.path.basename(match[0])
        result['script_path'] = os.path.dirname(match[0])
        #self.logger.info(str(result))
        before_match, match, after_match = self.match_one(credential_line, self.text)
        if match is not None:
          result['host'] = match[0].strip().split('.')[0].upper()
          result['account'] = match[1]
        else:
          self.logger.warn("Fails to get script file name in %s" % self.log_file_name)
          return {}

    if flags.has_key('TPT'):
        before_match, match, after_match = self.match_one(tpt_target_table, self.text)
        if match is not None:
            results['TPT_TARGET'] = match[0]
        before_match, match, after_match = self.match_one(tpt_source_file, self.text)
        if match is not None:
            results['TPT_SOURCE'] = match[0]

    if command_type in ['LI_INFA']:
        result['infa_folder'] = param_dict['infa_folder'].upper()
        result['infa_workflow'] = param_dict['infa_workflow'].upper()
        before_match, match, after_match = self.match_one(infa_run_id, self.text)
        if match is not None:
            result['infa_run_id'] = int(match[0])
        return result

    if command_type in ['LI_TPT_INSERT']:
        self.logger.info('LI_TPT_INSERT')
        before_match, match, after_match = self.match_one(script_name_line, self.text)
        if match is not None:
            result['script_name'] = os.path.basename(match[0])
            result['script_path'] = os.path.dirname(match[0])
        else:
            self.logger.error("Fails to get script file name in log file: %s", self.log_file_name)
            return {}
        tpt_source_file = Keyword('The input files are').suppress() + \
                          Literal('-').suppress() + \
                          Word(printables).setResultsName('source_file')
        before_match, match, after_match = self.match_one(tpt_source_file, self.text)
        if len(match) == 0:
            self.logger.error("Fails to get TPT load file name from log file: %s", self.log_file_name)
        result['table'] = []
        result['table'].append(ParseUtil.get_nas_location_abstraction(match[0]))
        result['table'][len(result['table']) - 1].update({
            'storage_type' : 'NAS',
            'table_type' : 'source',
            'full_path' : match[0]
        })

        before_match, match, after_match = self.match_one(tpt_target_rows, self.text)
        if match is None or len(match) == 0:
            self.logger.warn("Fails to get applied row count from log file: %s" % self.log_file_name)
            before_match, match, after_match = self.match_one(tpt_insert_rows, self.text)
            self.logger.info(str(match[0]))
            if match is None or len(match) == 0:
              insert_count = 0
            else:
              insert_count = match[0]
        else:
            insert_count = match[0]

        before_match, match, after_match = self.match_one(credential_line, self.text)
        if match is None or len(match) == 0:
            self.logger.error("Fails to get database name from log file: %s", self.log_file_name)
            return {}
        db_host = match.asDict()['db_host']
        user_account = match.asDict()['user_account']
        target_table_name = param_dict['TargetTable']
        schema_name, tab_name = ParseUtil.get_db_table_abstraction(target_table_name)
        result['table'].append(
            { 'table_type' : 'target',
              'storage_type' : 'Teradata',
              'schema' : schema_name,
              'table_name' : tab_name,
              'host' : db_host.split('.')[0].upper(),
              'insert_count' : insert_count,
              'account' : user_account })

    if command_type == 'LI_GETREPLACEMERGEGENERAL':
        result['cluster'] = param_dict['cluster_name'].upper()
        before_match, match, after_match = self.match_one(script_name_line, self.text)
        if match is not None:
            result['script_name'] = os.path.basename(match[0])
            result['script_path'] = os.path.dirname(match[0])
        result['table'] = []
        result['table'].append(ParseUtil.get_hdfs_location_abstraction(param_dict['hdfs_location']))
        result['table'][len(result['table']) - 1].update({
            'storage_type' : 'HDFS',
            'table_type' : 'source'})
        result['table'].append(ParseUtil.get_nas_location_abstraction(param_dict['nas_location']))
        result['table'][len(result['table']) - 1].update({
            'storage_type' : 'NAS',
            'table_type' : 'target',
            'full_path' : param_dict['nas_location']})
        return result


    if command_type == 'LI_PIG':
      try:
        result['cluster'] = param_dict['cluster_name'].upper()
        result['script_name'] = os.path.basename(param_dict['pig_script'])
        result['script_path'] = os.path.dirname(param_dict['pig_script'])
        #self.logger.info('LI_PIG')
        #self.logger.info(str(result))
        return result
      except:
        return {}

    if command_type == 'LI_CUBERT':
        try:
            kv_pair = Group(Keyword('-D').suppress() + Word(alphanums) + \
                            Suppress('=') + Word(printables))
            cubert_options = SkipTo(Literal('.params -D '), include=True).suppress() + \
                             Word(alphanums).suppress() + Suppress('=') + \
                             Word(printables).setResultsName('source_table') + \
                             OneOrMore(kv_pair).suppress() + \
                             SkipTo(Keyword('-D') + Word(alphanums+'_') + '=' + Word(printables) + \
                                    Keyword('-D'), include=False).suppress() + \
                             Keyword('-D').suppress() + Word(alphanums+'_').suppress() + Suppress('=') + \
                             Word(printables).setResultsName('target_table') + \
                             Suppress('-D startDateHour=') + Word(nums).setResultsName('start_partition') + \
                             Suppress('-D endDateHour=') + Word(nums).setResultsName('end_partition')

            (source_table_name, target_table_name, start_partition, end_partition) = \
                tuple( cubert_options.parseString(param_dict['cubert_options']) )

            result['cluster'] = param_dict['cluster_name']
            result['script_name'] = os.path.basename(param_dict['cubert_script'])
            result['script_path'] = os.path.dirname(param_dict['cubert_script'])
            result['table'] = [
                {'table_type': 'source',
                 'storage_type': 'HDFS',
                 'abstracted_path': source_table_name,
                 'start_partition': start_partition,
                 'end_partition': end_partition
                 },
                {'table_type': 'target',
                 'storage_type': 'HDFS',
                 'abstracted_path': target_table_name,
                 'start_partition': start_partition,
                 'end_partition': end_partition
                 }]
            return result
        except ParseException:
            (source_table_name, target_table_name, start_partition, end_partition) = ('', '', None, None)
            pass
        except KeyError:
            return {}

    if command_type in ['LI_WEBHDFS_GET']:
        result['cluster'] = param_dict['cluster_name'].upper()
        before_match, match, after_match = self.match_one(script_name_line, self.text)
        if match is not None:
            result['script_name'] = os.path.basename(match[0])
            result['script_path'] = os.path.dirname(match[0])
            result['script_type'] = 'Web HDFS'
        result['table'] = []
        result['table'].append(ParseUtil.get_hdfs_location_abstraction(param_dict['hdfs_path']))
        result['table'][len(result['table']) - 1].update({
            'storage_type' : 'HDFS',
            'table_type' : 'source',
            'full_path' : param_dict['hdfs_path']
        })
        result['table'].append(ParseUtil.get_nas_location_abstraction(param_dict['local_path']))
        result['table'][len(result['table']) - 1].update({
            'storage_type' : 'NAS',
            'table_type' : 'target',
            'full_path' : param_dict['local_path']})
        return result


    if command_type in ['LI_HADOOP']:
        result['cluster'] = param_dict['cluster_name'].upper()
        self.logger.info('cluster')
        self.logger.info(str(result['cluster']))
        script = re.search(r'jar\s+(.*?)\s+',param_dict['hadoop_command'].replace("'",""))
        if script is not None:
            if script.group(1).endswith('lassen-extract.jar'):
                result['script_name'] = os.path.basename(script.group(1))
                result['script_path'] = os.path.dirname(script.group(1))
                result['script_type'] = 'Lassen'
            else:
                return {}
        else:
            script = re.search(r'fs\s+-(mv|cp)', param_dict['hadoop_command'])
            if script is not None:
                before_match, match, after_match = self.match_one(script_name_line, self.text)
                if len(match) > 0:
                    result['script_name'] = os.path.basename(match[0])
                    result['script_path'] = os.path.dirname(match[0])
                    result['script_type'] = 'CMD'
        # Skip anything other than Lassen or HDFS move (till now)
        if script is None: return {}
        result['table'] = []
        if result['script_name'] == 'lassen-extract.jar':
            nl = Suppress(LineEnd())
            kw = Keyword('JOBSTATUS').suppress()
            job_status_line = kw + Literal(':').suppress() + \
                              Word(printables,excludeChars=';') + Literal(';').suppress() + \
                              delimitedList(Word(alphas) + Literal('=').suppress() + \
                                            Group(OneOrMore(~nl + Word(printables,excludeChars=';'))),';')
            #                                  delimitedList(Word(printables,excludeChars=';'),';').setResultsName('status')
            m = re.search(r'Schema obtained from\s+(.*?)\s+as\s+lassen', self.text)
            if m is not None:
                db_host = m.group(1).split('.')[0].upper()
            else:
                db_host = None
            source_table_lkp = {}
            for t,s,e in job_status_line.scanString(self.text):
                source_table_name = t.asList()[0].upper()
                skipped = False
                record_count = None
                hdfs_path = None
                for k,v in dict(zip(t.asList()[1::2],t.asList()[2::2])).items():
                    if k == 'Path':
                        if ' '.join(v).startswith('Snapshot already exists'):
                            hdfs_path = v[-1].replace('//','/')
                            record_count = 0
                        else:
                            hdfs_path = v[0].replace('//','/')
                    if k == 'Records':
                        record_count = v[0]
                    if k == 'status':
                        if v[0] in ['FAILURE']:
                            skipped = True
                            break
                        elif v[0] == 'NORECORDS':
                            record_count = 0
                        elif v[0] == 'NOTRUN':
                            record_count = 0
                if skipped: continue
                if not hdfs_path.strip().startswith('/'): continue
                if hdfs_path is not None and hdfs_path != 'null':
                    if source_table_lkp.has_key(source_table_name): continue
                    source_table_lkp[source_table_name] = True
                    result['table'].append(ParseUtil.get_hdfs_location_abstraction(hdfs_path))
                    result['table'][len(result['table']) -1].update({
                        'source_storage_type' : 'Teradata',
                        'source_schema' : 'DWH_STG',
                        'source_table_name' : source_table_name,
                        'source_database' : db_host,
                        'target_storage_type' : 'HDFS',
                        'record_count' : record_count})
        elif result['script_name'] == 'li_hadoop.sh':
            source, dest = param_dict['command_args'].replace('"','').split()[:2]
            result['table'].append(ParseUtil.get_hdfs_location_abstraction(source))
            result['table'][len(result['table']) - 1].update({
                'storage_type' : 'HDFS'
            })
            result['table'].append(ParseUtil.get_hdfs_location_abstraction(dest))
            result['table'][len(result['table']) - 1].update({
                'storage_type' : 'HDFS'
            })
            return result

    if command_type == 'KFK_SONORA_TD_LOAD':
        before_match, match, after_match = self.match_one(script_name_line, self.text)
        if match is not None:
            result['script_name'] = os.path.basename(match[0])
            result['script_path'] = os.path.dirname(match[0])
        else:
            self.logger.warn("Fails to get script name from log file: %s", self.log_file_name)
            return {}
        result['table'] = []
        before_match, match, after_match = self.match_one(merged_file_line, self.text)
        if match is not None:
            result['table'].append(ParseUtil.get_nas_location_abstraction(match[0]))
            result['table'][0].update(
                { 'table_type' : 'source',
                  'storage_type' : 'NAS',
                  'full_path': match[0]})
        else:
            before_match, match, after_match = self.match_one(processing_dir, self.text)
            if match is not None:
                result['table'].append(ParseUtil.get_nas_location_abstraction(match[0]))
                result['table'][0].update(
                    { 'table_type' : 'source',
                      'storage_type' : 'NAS',
                      'full_path': match[0]})
        before_match, match, after_match = self.match_one(insert_sql, self.text)
        if match is not None:
            result['table'].append(ParseUtil.get_nas_location_abstraction(match[1]))
            result['table'][0].update(
                { 'table_type' : 'source',
                  'storage_type' : 'NAS',
                  'full_path': match[1]})
            result['table'].append(
                { 'table_type' : 'target',
                  'storage_type' : 'Teradata',
                  'schema' : 'DWH_STG',
                  'table_name' : match[0],
                  'insert_count' : match[2],
                  'update_count' : match[3],
                  'delete_count' : match[4] })
            before_match, match, after_match = self.match_one(credential_line, self.text)
            if match is not None:
                result['table'][len(result['table']) - 1].update(
                    { 'host' : match[0].split('.')[0].upper(),
                      'account' : match[1] })
        else:
            self.logger.warn('Fails to find target table name for MLoad operation from log file: %s' % self.log_file_name)
            return {}

    if command_type in ['LINKEDIN_SHELL']:
        if param_dict.has_key('shell_script'):
            result['script_name'] = os.path.basename(param_dict['shell_script'])
            result['script_path'] = os.path.dirname(param_dict['shell_script'])
        else:
            self.logger.warn("Fails to get script name from log file: %s", self.log_file_name)
            return {}
        result['table'] = []
        # ssh -o GSSAPIDelegateCredentials=true data_svc@eat1-euchregw01.grid.linkedin.com hadoop fs -get /jobs/data_svc/sa_contentfiltering/ContentFilteringEvent/td_load/* - >/mnt/n001/data/sa_contentfiltering/ContentFilteringEvent.txt
        src_tgt_obj = re.search(r'^ssh\s+.*?\w+@(\S+)\s+\S+\s+fs\s+-get\s+\'{0,1}(\S+)\s+\S+\'{0,1}.*?>\s*(\S+)', self.text,re.M)
        if not src_tgt_obj is None:
            source_location = re.sub(r'/\*$','',src_tgt_obj.group(2))
            result['table'].append(ParseUtil.get_hdfs_location_abstraction(source_location))
            result['cluster'] = param_dict['params2'] # risky code to look at a specific parameter value
            result['table'][0].update(
                {'table_type' : 'source',
                 'full_path': source_location,
                 'storage_type' : 'HDFS'})
            target_location = re.sub(r"'$",'',src_tgt_obj.group(3))
            result['table'].append(ParseUtil.get_nas_location_abstraction(target_location))
            result['table'][1].update(
                {'table_type' : 'target',
                 'full_path' : target_location,
                 'storage_type' : 'NAS'
                 })

    if command_type in ['KFK_SONORA_HADOOP_GET']:
        try:
            result['cluster'] = param_dict['cluster_name'].upper()
        except KeyError:
            self.logger.warn('Fails to get cluster name from log file: %s' % self.log_file_name)
            return {}
        before_match, match, after_match = self.match_one(script_name_line, self.text)
        if match is not None:
            result['script_name'] = os.path.basename(match[0])
            result['script_path'] = os.path.dirname(match[0])
        else:
            self.logger.warn("Fails to get script name from log file: %s", self.log_file_name)
            return {}
        result['table'] = []
        src_tgt_obj = re.search(r'ssh\s+.*?\w+@(\S+)\s+\S+\s+fs\s+-get\s+\'(\S+)\s+\S+\'\s+>\s+(\S+)', self.text, re.M)
        final_tgt_obj = re.search(r'cat\s+(.*?)\s*\>\s*(.*)', self.text, re.M)
        if not src_tgt_obj is None:
            source_location = src_tgt_obj.group(2).replace('/*','')
            result['table'].append(ParseUtil.get_hdfs_location_abstraction(source_location))
            result['table'][0].update(
                {'table_type' : 'source',
                 'full_path': source_location,
                 'storage_type' : 'HDFS'})
            if final_tgt_obj is not None:
                result['table'].append(ParseUtil.get_nas_location_abstraction(final_tgt_obj.group(2)))
                result['table'][1].update(
                    {'table_type' : 'target',
                     'full_path' : final_tgt_obj.group(2),
                     'storage_type' : 'NAS'
                     })
            else:
                result['table'].append(ParseUtil.get_nas_location_abstraction(final_tgt_obj.group(3)))
                result['table'][1].update(
                    {'table_type' : 'target',
                     'full_path' : final_tgt_obj.group(3),
                     'storage_type' : 'NAS'
                     })
        else:
            src_tgt_obj = re.search(r'hadoop\s+fs\s+-(get|cat)\s+(\S+)\s+.*?>\s{0,}(\S+)',self.text,re.M)
            source_location = src_tgt_obj.group(2).replace('/*','')
            if not src_tgt_obj is None:
                result['table'].append(ParseUtil.get_hdfs_location_abstraction(source_location))
                result['table'][0].update(
                    {'table_type' : 'source',
                     'full_path': source_location,
                     'storage_type' : 'HDFS'})
                result['table'].append(ParseUtil.get_nas_location_abstraction(src_tgt_obj.group(3)))
                result['table'][1].update(
                    {'table_type' : 'target',
                     'full_path' : src_tgt_obj.group(3),
                     'storage_type' : 'NAS'
                     })
            else:
                self.logger.warn('Fails to retrieve source & target file names from log file: %s' % log_file)
                return {}
    return result

if __name__ == "__main__":
  props = sys.argv[1]
  aw_parser = AppworxLogParser()
