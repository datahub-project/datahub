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

def get_db_table_abstraction(table_name):
  parsed_table_name = table_name.split('.')
  if len(parsed_table_name) == 1:
    schema_name = 'DWH_STG'
    tab_name = parsed_table_name[0].upper()
  else:
    schema_name = parsed_table_name[0].upper()
    tab_name = parsed_table_name[1].upper()
  if tab_name.startswith('V_'):
    tab_name = tab_name.replace('V_', '')
  return [schema_name, tab_name]

def value_from_cmd_line(param, key_values):
  """
  Routine to parse Pig command line parameters. Returns dictionary key_values
  Dictionary key_values may contain existing entry from Pig parameter file
  Value for key will get overwritten after command line is processed
  """
  if not param:
    return
  match_obj = re.match(r'"(.*)"', param)
  if match_obj:
    param_wo_quote = match_obj.group(1)
  else:
    param_wo_quote = param

  key_str = Word(alphanums + '_')
  value_str = Word(printables, excludeChars=';')
  kv = key_str.setResultsName('key_str', listAllMatches=True) + \
       Literal('=').suppress() + \
       value_str.setResultsName('value_str', listAllMatches=True)
  for tokens, start, stop in kv.scanString(param_wo_quote):
      key_values[tokens.asList()[0]] = tokens.asList()[1]

def get_nas_location_abstraction(location):
  m = re.match(r'(.*?)/(\d{10})-(\d{10}).*$', location)
  if m is not None:
    data_start_date = m.group(2)
    data_end_date = m.group(3)
    dir_name = m.group(1)
    return {'abstracted_path': dir_name, \
            'format_mask': 'YYYYMMDDHH', \
            'data_start_date': data_start_date, \
            'data_end_date': data_end_date, \
            'frequency': 'hourly',
            'start_partition': data_start_date, \
            'end_partition': data_end_date}

  m = re.match(r'(.*?)/(\d{8})-(\d{8}).*$', location)
  if m is not None:
    data_start_date = m.group(2)
    data_end_date = m.group(3)
    dir_name = m.group(1)
    return {'abstracted_path': dir_name, \
            'format_mask': 'YYYYMMDD', \
            'data_start_date': data_start_date, \
            'data_end_date': data_end_date, \
            'frequency': 'daily', \
            'start_partition': data_start_date, \
            'end_partition': data_end_date}

  m = re.match(r'(.*?)/(\d{8})-(\d{8}).*$', location)

  # Handle exceptional situations like
  # /mnt/n001/data/kfk_webtrack/transfer/AGG_DAILY_CAPACTIVITY/00-00/merged_file.gz

  m = re.match(r'(.*?)/(\d{1,})-(\d{1,}).*$', location)
  if m is not None:
    data_start_date = m.group(2)
    data_end_date = m.group(3)
    dir_name = m.group(1)
    return {'abstracted_path': dir_name, \
            'format_mask': None, \
            'data_start_date': None, \
            'data_end_date': None, \
            'frequency': None, \
            'start_partition': None, \
            'end_partition': None}

  return {'abstracted_path': location, \
          'format_mask': None, \
          'data_start_date': None, \
          'data_end_date': None, \
          'frequency': None, \
          'start_partition': None, \
          'end_partition': None}

def get_hdfs_location_abstraction(location):

  #remove header in simple_parser

  header = re.match(r'(hdfs|hive|kafka)://.*?/', location, re.I)
  storage_type = 'HDFS'
  if header and header.group(1) and header.group(1).lower() == 'hive':
    storage_type = 'HIVE'
  stripped_location = re.sub(r'(hdfs|hive|kafka)://.*?/', '', location)  # remove hdfs://...
  stripped_location = re.sub(r'kafka://.*?/', '', stripped_location)  # remove kafka://...
  stripped_location = re.sub(r'/\*$', '', stripped_location)  # Removing any trailing glob characters
  if stripped_location.endswith('/'):
    stripped_location = stripped_location[:-1]

  if location != stripped_location and not stripped_location.startswith('/'):
    stripped_location = '/' + stripped_location

  stripped_location = re.sub(r'\/{2,}','/',stripped_location)
  #print stripped_location

  part_match = re.findall(r'(.*?)/00-00.gz', stripped_location)
  if len(part_match) > 0:
    return dict(abstracted_path=part_match[0], format_mask=None, frequency='snapshot',
                start_partition=None, end_partition=None,
                explicit_partition=False, full_path=location, storage_type=storage_type)

  part_match = re.findall(r'(.*?)/{1,}null', stripped_location)
  if len(part_match) > 0:
    return dict(abstracted_path=part_match[0], format_mask=None, frequency='snapshot',
                start_partition=None, end_partition=None,
                 explicit_partition=False, full_path=location, storage_type=storage_type)

  # /jobs/data_svc/etl/tracking/lrf/agg_bzops_cmp_conn/201410-201410.gz
  part_match = re.findall(r'(.*?)/(\d{6})-(\d{6}).gz', stripped_location)
  if len(part_match) > 0:
    return dict(abstracted_path=part_match[0][0], format_mask=None, frequency='monthly',
                start_partition=part_match[0][1], end_partition=part_match[0][2],
                explicit_partition=False, full_path=location, storage_type=storage_type)

  # /jobs/data_svc/responsys/inputfiles/responsys_open/responsys_open_merged_file_20141110091511_concat.txt
  part_match = re.findall(r'(.*?)\_(\d{14})(.*)',stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0] + '_YYYYMMDDHHMISS' + part_match[0][2], \
            'format_mask': None, 'frequency': 'snapshot', 'explicit_partition': False,
            'start_partition': None, 'end_partition': None, 'full_path': location, 'storage_type': storage_type}

  part_match = re.findall(r'(.*?)(/tmp)?/hourly/(\d{4}/\d{2}/\d{2}/\d{2})', stripped_location)
  if len(part_match) > 0:
    return dict(abstracted_path=part_match[0][0], format_mask='YYYYMMDDHH', frequency='hourly',
                start_partition=part_match[0][2].replace("/", ""), end_partition=part_match[0][2].replace("/", ""),
                explicit_partition=True, full_path=location, storage_type=storage_type)

  part_match = re.findall(r'(.*?)(/tmp)?/daily/(\d{4}/\d{2}/\d{2})', stripped_location)
  if len(part_match) > 0:
    return dict(abstracted_path=part_match[0][0], format_mask='YYYYMMDD', frequency='daily',
                start_partition=part_match[0][2].replace("/", ""), end_partition=part_match[0][2].replace("/", ""),
                explicit_partition=True, full_path=location, storage_type=storage_type)

  part_match = re.findall(r'(.*?)(/tmp)?/(daily|monthly)?/(\d{4}/\d{2})', stripped_location)
  if len(part_match) > 0:
    return dict(abstracted_path=part_match[0][0], format_mask='YYYYMM', frequency='monthly',
                start_partition=part_match[0][3].replace("/", ""), end_partition=part_match[0][3].replace("/", ""),
                explicit_partition=True, full_path=location, storage_type=storage_type)

  #data/derived/member/summary/2014-05-06-05-16
  part_match = re.findall(r'(.*?)/(\d{4}-\d{2}-\d{2}-\d{2}-\d{2})', stripped_location)
  if len(part_match) > 0:
    return dict(abstracted_path=part_match[0][0], format_mask='YYYYMMDD', frequency='daily', start_partition=None,
                end_partition=part_match[0][1].replace('-', ''), explicit_partition=True, full_path=location,
                storage_type=storage_type)

  part_match = re.findall(r'(.*?)(/tmp)?/(daily/)?(\d{4}/\d{2}/\d{2})(/|$)', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDD', \
            'frequency': 'daily', \
            'start_partition': part_match[0][3].replace("/", ""), \
            'end_partition': part_match[0][3].replace("/", ""), \
            'explicit_partition': True,
            'full_path': location,
            'storage_type':storage_type}


  #/data/derived/emerald/labels/2014-07-18
  part_match = re.findall(r'(.*?)(/tmp)?/(\d{4}-\d{2}-\d{2})(/|$)', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDD', \
            'frequency': 'daily', \
            'start_partition': part_match[0][2].replace("-", ""), \
            'end_partition': part_match[0][2].replace("-", ""), \
            'explicit_partition': True,
            'full_path': location,
            'storage_type':storage_type}

  #/user/yyang/homepage/contextual/framework/lego/dormant2/20140716/lixTreatment
  part_match = re.findall(r'(.*?)/(\d{8})(/|$)', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDD', \
            'frequency': 'daily', \
            'start_partition': part_match[0][1], \
            'end_partition': part_match[0][1], \
            'explicit_partition': True,
            'full_path': location,
            'storage_type':storage_type}


  #/data/derived/connection_strength/p_invitation/2014-07-18-12
  part_match = re.findall(r'(.*?)(/tmp)?/(\d{4}-\d{2}-\d{2}-\d{2})(/|$)', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDDHH', \
            'frequency': 'hourly', \
            'start_partition': part_match[0][2].replace("-", ""), \
            'end_partition': part_match[0][2].replace("-", ""), \
            'explicit_partition': True,
            'full_path': location,
            'storage_type':storage_type}

  part_match = re.findall(r'(.*?)(/tmp)?/(\d{4}/\d{2}/\d{2}/\d{2})(/|$)', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDD', \
            'frequency': 'hourly', \
            'explicit_partition': False,
            'start_partition': part_match[0][2].replace("/", ""), \
            'end_partition': part_match[0][2].replace("/", ""), \
            'full_path': location,
            'storage_type':storage_type}

  #/projects/dwh/dwh_dim/dim_page_key/20140718214002
  part_match = re.findall(r'(.*?)/(\d{10})(/|$)', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDDHHMISS', \
            'frequency': 'snapshot', \
            'explicit_partition': False,
            'start_partition': part_match[0][2], \
            'end_partition': part_match[0][2], \
            'full_path': location,
            'storage_type':storage_type}


  part_match = re.findall(r'(.*?)/\*(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-\d{3})\*', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDDHHMISSFFF', \
            'frequency': 'snapshot', \
            'explicit_partition': False,
            'start_partition': None, \
            'end_partition': part_match[0][1].replace("-", ""), \
            'full_path': location,
            'storage_type':storage_type}

  part_match = re.findall(r'(.*?)/(\d{4}-\d{2}-\d{2})T(\d{2}-\d{2}-\d{2})(\+\d{4})', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDDHHMISSFFFF', \
            'frequency': 'snapshot', \
            'explicit_partition': False,
            'start_partition': None, \
            'end_partition': part_match[0][1].replace("-", "") + part_match[0][2].replace("-", "") , \
            'time_zone' :'UTC'+ part_match[0][3] ,
            'full_path': location,
            'storage_type':storage_type}

  part_match = re.findall(r'(.*?)/(\d{14})(/|$)', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDDHHMISS', \
            'frequency': 'snapshot', \
            'explicit_partition': False,
            'start_partition': None, \
            'end_partition': part_match[0][1], \
            'full_path': location,
            'storage_type':storage_type}

  part_match = re.findall(r'(.*?)/datepartition=\{?(\d{4}-\d{2}-\d{2})\}?', stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0], \
            'format_mask': 'YYYYMMDD', \
            'frequency': 'daily', \
            'explicit_partition': False,
            'start_partition': part_match[0][1].replace("-", ""), \
            'end_partition': part_match[0][1].replace("-", ""), \
            'full_path': location,
            'storage_type':storage_type}

  # /jobs/data_svc/sa_contentfiltering/ContentFilteringEvent/Content_member_details_report_2014110923_2014110923
  part_match = re.findall(r'(.*?)\_(\d{10})\_(\d{10})',stripped_location)
  if len(part_match) > 0:
    return {'abstracted_path': part_match[0][0] + '_YYYYMMDD', \
            'format_mask': 'YYYYMMDD', \
            'frequency': 'daily', \
            'explicit_partition': False,
            'start_partition': part_match[0][1], \
            'end_partition': part_match[0][2], \
            'full_path': location,
            'storage_type':storage_type}



  path_components = stripped_location.split('/')
  number_word = Word(nums).setResultsName('nums')
  snapshot_ts_format = (number_word + \
                        Literal('-PT-').suppress() +
                        number_word)

  day_ts_format = Word(nums, exact=14).setResultsName('daily_ts_file')
  hour_format = Word(nums, exact=10).setResultsName('hourly_file')
  day_format = Word(nums, exact=8).setResultsName('daily_file')
  unknown_format = Word(nums, exact=2).setResultsName('unknown_file')

  t_format = day_ts_format | hour_format | day_format | unknown_format
  file_name = snapshot_ts_format.setResultsName('snapshot_ts_file') | \
              (t_format + Literal('-').suppress() + t_format).setResultsName('date_range') + \
              Optional(Literal('.') + restOfLine).setResultsName('file_ext')

  abstracted_path = stripped_location

  freq = None
  format_mask = None
  start_date = None
  end_date = None
  #print (str(path_components))
  for index, path_component in enumerate(path_components):
    for tokens, start, stop in file_name.scanString(path_component):
      #print str(tokens.asDict())
      file_ext = tokens.asDict()['file_ext'] if tokens.asDict().has_key('file_ext') else None
      if tokens.asDict().has_key('snapshot_ts_file'):
        abstracted_path = '/'.join(path_components[:index])
        format_mask = 'YYYYMMDDHHMISS'
        freq = 'snapshot'
        if isinstance(tokens.asDict()['snapshot_ts_file'], dict):
          tm = int((tokens.asDict()['snapshot_ts_file']).values()[0])
        else:
          tm = int(tokens.asDict()['snapshot_ts_file'][0])
        try:
          end_date = time.strftime('%Y%m%d%H%M%S',
                                 time.localtime( tm/ 1000))
        except:
          pass
      elif tokens.asDict().has_key('hourly_file'):
        abstracted_path = '/'.join(path_components[:index])
        format_mask = 'YYYYMMDDHH'
        freq = 'hourly'
        try:
          start_date = tokens.asDict()['date_range'][0]
        except:
          pass
        try:
          end_date = tokens.asDict()['date_range'][1]
        except:
          end_date = start_date
      elif tokens.asDict().has_key('daily_file'):
        abstracted_path = '/'.join(path_components[:index])
        format_mask = 'YYYYMMDD'
        freq = 'daily'
        try:
          #if isinstance(tokens.asDict()['date_range'], dict):
          #    start_date = ((tokens.asDict()['date_range']).values())[0]
          #else:
          start_date = tokens.asDict()['date_range'][0]
        except:
          pass

      elif tokens.asDict().has_key('daily_ts_file'):
        abstracted_path = '/'.join(path_components[:index])
        format_mask = 'YYYYMMDDHHMISS'
        freq = 'snapshot'
        try:
          end_date = tokens.asDict()['date_range'][0]
        except:
          end_date = start_date
          pass
      elif tokens.asDict().has_key('unknown_file'):
        #abstracted_path = '/'.join(path_components[:index])
        format_mask = None
        freq = None
        start_date = None
        end_date = None

  return {'abstracted_path': abstracted_path, \
            'format_mask': format_mask, \
            'frequency': freq, \
            'explicit_partition': False, \
            'start_partition': start_date, \
            'end_partition': end_date, \
            'full_path': location,
            'storage_type':storage_type}
