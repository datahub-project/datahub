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

from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
import DbUtil
import sys
import json
import urllib
import urllib2
from org.slf4j import LoggerFactory


class ElasticSearchIndex():
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.elasticsearch_index_url = args[Constant.WH_ELASTICSEARCH_URL_KEY]
    self.elasticsearch_port = args[Constant.WH_ELASTICSEARCH_PORT_KEY]
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()

  def bulk_insert(self, params, url):
    try:
      req = urllib2.Request(url=url)
      req.add_header('Content-type', 'application/json')
      req.get_method = lambda: "PUT"
      req.add_data('\n'.join(params) + '\n')
      response = urllib2.urlopen(req)
      data = json.load(response)
      if str(data['errors']) != 'False':
        self.logger.info(str(data))
    except urllib2.HTTPError as e:
      self.logger.error(str(e.code))
      self.logger.error(e.read())

  def update_dataset_field(self, last_time=None):
      if last_time:
          sql = """
            SELECT * FROM dict_field_detail WHERE modified >= DATE_SUB(%s, INTERVAL 1 HOUR)
            """ % last_time
      else:
          sql = """
            SELECT * FROM dict_field_detail
          """

      comment_query = """
        SELECT d.field_id, d.dataset_id, f.comment FROM dict_dataset_field_comment d
        LEFT JOIN field_comments f ON d.comment_id = f.id WHERE d.field_id = %d
        """
      url = self.elasticsearch_index_url + ':' + str(self.elasticsearch_port)  + '/wherehows/field/_bulk'
      params = []
      self.wh_cursor.execute(sql)
      comment_cursor = self.wh_con.cursor()
      description = [x[0] for x in self.wh_cursor.description]
      row_count = 1
      for result in self.wh_cursor:
          row = dict(zip(description, result))
          comment_cursor.execute(comment_query % long(row['field_id']))
          comments = []
          comment_description = [x[0] for x in comment_cursor.description]
          for comment_result in comment_cursor:
            comment_row = dict(zip(comment_description, comment_result))
            comments.append(comment_row['comment'])
          params.append('{ "index": { "_id": ' +
                        str(row['field_id']) + ', "parent": ' + str(row['dataset_id']) + '  }}')
          if len(comments) > 0:
            params.append(
                """{ "comments": %s, "dataset_id": %d, "sort_id": %d, "field_name": "%s", "parent_path": "%s"}"""
                % (json.dumps(comments) if comments else '', row['dataset_id'] if row['dataset_id'] else 0,
                   row['sort_id'] if row['sort_id'] else 0,
                   row['field_name'] if row['field_name'] else '', row['parent_path'] if row['parent_path'] else ''))
          else:
            params.append(
                """{ "comments": "", "dataset_id": %d, "sort_id": %d, "field_name": "%s", "parent_path": "%s"}"""
                % (row['dataset_id'] if row['dataset_id'] else 0, row['sort_id'] if row['sort_id'] else 0,
                   row['field_name'] if row['field_name'] else '', row['parent_path'] if row['parent_path'] else ''))
          if row_count % 1000 == 0:
              self.bulk_insert(params, url)
              params = []
          row_count += 1
      if len(params) > 0:
          self.bulk_insert(params, url)

      comment_cursor.close()

  def update_comment(self, last_time=None):
    if last_time:
        sql = """
          SELECT * FROM comments WHERE modified >= DATE_SUB(%s, INTERVAL 1 HOUR)
          """ % last_time
    else:
        sql = """
          SELECT * FROM comments
          """

    url = self.elasticsearch_index_url + ':' + str(self.elasticsearch_port) +  '/wherehows/comment/_bulk'
    params = []
    self.wh_cursor.execute(sql)
    row_count = 1
    description = [x[0] for x in self.wh_cursor.description]
    for result in self.wh_cursor:
      row = dict(zip(description, result))
      params.append('{ "index": { "_id": ' + str(row['id']) + ', "parent": ' + str(row['dataset_id']) + '  }}')
      params.append(
          """{ "text": %s, "user_id": %d, "dataset_id": %d, "comment_type": "%s"}"""
          % (json.dumps(row['text']) if row['text'] else '', row['user_id'] if row['user_id'] else 0,
             row['dataset_id'] if row['dataset_id'] else 0, row['comment_type'] if row['comment_type'] else ''))
      if row_count % 1000 == 0:
        self.bulk_insert(params, url)
        params = []
      row_count += 1
    if len(params) > 0:
      self.bulk_insert(params, url)

  def update_dataset(self, last_unixtime=None):
    if last_unixtime:
        sql = """
          SELECT * FROM dict_dataset WHERE from_unixtime(modified_time) >= DATE_SUB(from_unixtime(%f), INTERVAL 1 HOUR)
          """ % last_unixtime
    else:
        sql = """
        SELECT * FROM dict_dataset
        """
    url = self.elasticsearch_index_url + ':' + str(self.elasticsearch_port) +  '/wherehows/dataset/_bulk'
    params = []
    self.wh_cursor.execute(sql)
    description = [x[0] for x in self.wh_cursor.description]
    row_count = 1
    for result in self.wh_cursor:
      row = dict(zip(description, result))
      params.append('{ "index": { "_id": ' + str(row['id']) + ' }}')
      params.append(
          """{ "name": "%s", "source": "%s", "urn": "%s", "location_prefix": "%s", "parent_name": "%s","schema_type": "%s", "properties": %s, "schema": %s , "fields": %s}"""
          % (row['name'] if row['name'] else '', row['source'] if row['source'] else '',
             row['urn'] if row['urn'] else '', row['location_prefix'] if row['location_prefix'] else '',
             row['parent_name'] if row['parent_name'] else '', row['schema_type'] if row['schema_type'] else '',
             json.dumps(row['properties'])  if row['properties'] else '',
             json.dumps(row['schema'])  if row['schema'] else '', json.dumps(row['fields'])  if row['fields'] else ''))
      if row_count % 1000 == 0:
        self.bulk_insert(params, url)
        self.logger.info('dataset' + str(row_count))
        params = []
      row_count += 1
    if len(params) > 0:
      self.bulk_insert(params, url)
      self.logger.info('dataset' + str(len(params)))

  def update_metric(self):
      sql = """
        SELECT * FROM dict_business_metric
        """
      url = self.elasticsearch_index_url + ':' + str(self.elasticsearch_port) +  '/wherehows/metric/_bulk'
      params = []
      self.wh_cursor.execute(sql)
      description = [x[0] for x in self.wh_cursor.description]
      row_count = 1
      for result in self.wh_cursor:
          row = dict(zip(description, result))
          params.append('{ "index": { "_id": ' + str(row['metric_id']) + '  }}')
          params.append(
              """{"metric_id": %d,  "metric_name": %s, "metric_description": %s, "dashboard_name": %s, "metric_group": %s, "metric_category": %s, "metric_sub_category": %s, "metric_level": %s, "metric_source_type": %s, "metric_source": %s, "metric_source_dataset_id": %d, "metric_ref_id_type": %s, "metric_ref_id": %s, "metric_type": %s, "metric_additive_type": %s, "metric_grain": %s, "metric_display_factor": %f, "metric_display_factor_sym": %s, "metric_good_direction": %s, "metric_formula": %s, "dimensions": %s, "owners": %s, "tags": %s, "urn": %s, "metric_url": %s, "wiki_url": %s, "scm_url": %s}"""
              % (row['metric_id'], json.dumps(row['metric_name']) if row['metric_name'] else json.dumps(''),
                 json.dumps(row['metric_description']) if row['metric_description'] else json.dumps(''),
                 json.dumps(row['dashboard_name']) if row['dashboard_name'] else json.dumps(''),
                 json.dumps(row['metric_group']) if row['metric_group'] else json.dumps(''),
                 json.dumps(row['metric_category']) if row['metric_category'] else json.dumps(''),
                 json.dumps(row['metric_sub_category']) if row['metric_sub_category'] else json.dumps(''),
                 json.dumps(row['metric_level']) if row['metric_level'] else json.dumps(''),
                 json.dumps(row['metric_source_type']) if row['metric_source_type'] else json.dumps(''),
                 json.dumps(row['metric_source']) if row['metric_source'] else json.dumps(''),
                 row['metric_source_dataset_id'] if row['metric_source_dataset_id'] else 0,
                 json.dumps(row['metric_ref_id_type']) if row['metric_ref_id_type'] else json.dumps(''),
                 json.dumps(row['metric_ref_id']) if row['metric_ref_id'] else json.dumps(''),
                 json.dumps(row['metric_type']) if row['metric_type'] else json.dumps(''),
                 json.dumps(row['metric_additive_type']) if row['metric_additive_type'] else json.dumps(''),
                 json.dumps(row['metric_grain']) if row['metric_grain'] else json.dumps(''),
                 row['metric_display_factor'] if row['metric_display_factor'] else 0.0,
                 json.dumps(row['metric_display_factor_sym']) if row['metric_display_factor_sym'] else json.dumps(''),
                 json.dumps(row['metric_good_direction']) if row['metric_good_direction'] else json.dumps(''),
                 json.dumps(row['metric_formula']) if row['metric_formula'] else json.dumps(''),
                 json.dumps(row['dimensions']) if row['dimensions'] else json.dumps(''),
                 json.dumps(row['owners']) if row['owners'] else json.dumps(''),
                 json.dumps(row['tags']) if row['tags'] else json.dumps(''),
                 json.dumps(row['urn']) if row['urn'] else json.dumps(''),
                 json.dumps(row['metric_url']) if row['metric_url'] else json.dumps(''),
                 json.dumps(row['wiki_url']) if row['wiki_url'] else json.dumps(''),
                 json.dumps(row['scm_url']) if row['scm_url'] else json.dumps('')))
          if row_count % 1000 == 0:
            self.bulk_insert(params, url)
            params = []
          row_count += 1
      if len(params) > 0:
          self.bulk_insert(params, url)

  def update_flow_jobs(self, last_unixtime=None):
      if last_unixtime:
          flow_sql = """
            SELECT a.app_code, f.* FROM flow f JOIN cfg_application a on f.app_id = a.app_id
            WHERE from_unixtime(modified_time) >= DATE_SUB(from_unixtime(%f), INTERVAL 1 HOUR)
            """ % last_unixtime
      else:
          flow_sql = """
            SELECT a.app_code, f.* FROM flow f JOIN cfg_application a on f.app_id = a.app_id
            """
      job_sql = """
        SELECT * FROM flow_job WHERE app_id = %d and flow_id = %d
        """
      url = self.elasticsearch_index_url + ':' + str(self.elasticsearch_port) +  '/wherehows/flow_jobs/_bulk'
      params = []
      self.wh_cursor.execute(flow_sql)
      job_cursor = self.wh_con.cursor()
      description = [x[0] for x in self.wh_cursor.description]
      row_count = 1
      for result in self.wh_cursor:
          row = dict(zip(description, result))
          job_cursor.execute(job_sql %(long(row['app_id']), long(row['flow_id'])))
          jobs = []
          job_description = [x[0] for x in job_cursor.description]
          for job_result in job_cursor:
              job_row = dict(zip(job_description, job_result))
              jobs.append({"app_id": job_row['app_id'], "flow_id": job_row['flow_id'], "job_id": job_row['job_id'],
                    "job_name": job_row['job_name'] if job_row['job_name'] else '',
                    "job_path": job_row['job_path'] if job_row['job_path'] else '',
                    "job_type_id": job_row['job_type_id'],
                    "job_type": job_row['job_type'] if job_row['job_type'] else '',
                    "pre_jobs": job_row['pre_jobs'] if job_row['pre_jobs'] else '',
                    "post_jobs": job_row['post_jobs'] if job_row['post_jobs'] else '',
                    "is_current": job_row['is_current'] if job_row['is_current'] else '',
                    "is_first": job_row['is_first'] if job_row['is_first'] else '',
                    "is_last": job_row['is_last'] if job_row['is_last'] else ''})

          params.append('{ "index": { "_id": ' + str(long(row['flow_id'])*10000 + long(row['app_id'])) + '  }}')
          if len(jobs) > 0:
              params.append(
                  """{"app_id": %d,  "flow_id": %d, "app_code": "%s", "flow_name": "%s", "flow_group": "%s", "flow_path": "%s", "flow_level": %d, "is_active": "%s", "is_scheduled": "%s", "pre_flows": "%s", "jobs": %s}"""
                  % (row['app_id'], row['flow_id'], row['app_code'] if row['app_code'] else '',
                     row['flow_name'] if row['flow_name'] else '', row['flow_group'] if row['flow_group'] else '',
                     row['flow_path'] if row['flow_path'] else '', row['flow_level'],
                     row['is_active'] if row['is_active'] else '', row['is_scheduled'] if row['is_scheduled'] else '',
                     row['pre_flows'] if row['pre_flows'] else '', json.dumps(jobs)))
          else:
              params.append(
                  """{"app_id": %d,  "flow_id": %d, "app_code": "%s", "flow_name": "%s", "flow_group": "%s", "flow_path": "%s", "flow_level": %d, "is_active": "%s", "is_scheduled": "%s", "pre_flows": "%s", "jobs": ""}"""
                  % (row['app_id'], row['flow_id'], row['app_code'] if row['app_code'] else '',
                     row['flow_name'] if row['flow_name'] else '', row['flow_group'] if row['flow_group'] else '',
                     row['flow_path'] if row['flow_path'] else '', row['flow_level'],
                     row['is_active'] if row['is_active'] else '', row['is_scheduled'] if row['is_scheduled'] else '',
                     row['pre_flows'] if row['pre_flows'] else ''))
          if row_count % 1000 == 0:
              self.bulk_insert(params, url)
              self.logger.info('flow jobs' + str(row_count))
              params = []
          row_count += 1
      if len(params) > 0:
          self.logger.info('flow_jobs' + str(len(params)))
          self.bulk_insert(params, url)

      job_cursor.close()

  def run(self):

    try:
      self.update_dataset()
      self.update_comment()
      self.update_dataset_field()
      self.update_flow_jobs()
      self.update_metric()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

if __name__ == "__main__":
  props = sys.argv[1]
  esi = ElasticSearchIndex(props)
  esi.run()
