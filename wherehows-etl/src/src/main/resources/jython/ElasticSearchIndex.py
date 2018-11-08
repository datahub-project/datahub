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
from org.slf4j import LoggerFactory
import sys, json
import urllib2
import time

class ElasticSearchIndex():
    def __init__(self, args):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
        self.elasticsearch_server_url = args[Constant.ELASTICSEARCH_URL_KEY]
        self.elasticsearch_port = args[Constant.ELASTICSEARCH_PORT_KEY]

        if Constant.ELASTICSEARCH_INDEX_KEY not in args:
            self.elasticsearch_index = "wherehows"
        else:
            self.elasticsearch_index = args[Constant.ELASTICSEARCH_INDEX_KEY]

        self.index_mapping_file = args[Constant.WH_ELASTICSEARCH_INDEX_MAPPING_FILE]

        self.bulk_chunk_size = int(args[Constant.ELASTICSEARCH_BULK_INSERT_SIZE]) # bulk insert size to elastic search engine
        self.es_url_request_timeout = int(args[Constant.ELASTICSEARCH_URL_REQUEST_TIMEOUT]) # url to post data to elastic search engine request time out
        self.max_retry_times = int(args[Constant.WH_DB_MAX_RETRY_TIMES]) # max times for db re-connection when lost during fetching source data


        self.base_url = self.elasticsearch_server_url + ':' + str(self.elasticsearch_port) + '/'
        self.logger.info(self.base_url)

        self.old_index = []
        self.new_index = []

        self.databaseConnect(args)

    def databaseConnect(self, args):
        self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                     args[Constant.WH_DB_USERNAME_KEY],
                                     args[Constant.WH_DB_PASSWORD_KEY],
                                     args[Constant.WH_DB_DRIVER_KEY])
        self.wh_cursor = self.wh_con.cursor(1)

    def bulk_insert(self, params, url):
        try:
            req = urllib2.Request(url=url)
            req.add_header('Content-type', 'application/json')
            req.get_method = lambda: "PUT"
            req.add_data('\n'.join(params) + '\n')
            self.logger.info(url)
            response = urllib2.urlopen(req, timeout=self.es_url_request_timeout)
            data = json.load(response)
            if str(data['errors']) != 'False':
                self.logger.info(str(data))
        except urllib2.HTTPError as e:
            self.logger.error(str(e.code))
            self.logger.error(e.read())
        except Exception as e:
            self.logger.error(str(e))

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
        url = self.elasticsearch_server_url + ':' + str(
            self.elasticsearch_port) + '/' + self.new_index + '/field/_bulk'
        params = []
        attempts = 0
        while attempts < self.max_retry_times:
            try:
                self.wh_cursor.execute(sql)
                comment_cursor = self.wh_con.cursor(1)
                description = [x[0] for x in self.wh_cursor.description]
                row_count = 1
                result = self.wh_cursor.fetchone()
                while result:
                    row = dict(zip(description, result))
                    comment_cursor.execute(comment_query % long(row['field_id']))
                    comments = []
                    comment_description = [x[0] for x in comment_cursor.description]
                    comment_result = comment_cursor.fetchone()
                    while comment_result:
                        comment_row = dict(zip(comment_description, comment_result))
                        comments.append(comment_row['comment'])
                        comment_result = comment_cursor.fetchone()
                    params.append('{ "index": { "_id": ' +
                                  str(row['field_id']) + ', "parent": ' + str(row['dataset_id']) + '  }}')
                    comments_detail = {
                        'comments': comments,
                        'dataset_id': row['dataset_id'],
                        'sort_id': row['sort_id'],
                        'field_name': row['field_name'],
                        'parent_path': row['parent_path']
                    }
                    params.append(json.dumps(comments_detail))

                    if row_count % self.bulk_chunk_size == 0:
                        self.bulk_insert(params, url)
                        self.logger.info('dataset field ' + str(row_count))
                        self.wh_con.commit()  # commit regularly to avoid long open transaction
                        params = []
                    row_count += 1
                    result = self.wh_cursor.fetchone()
                if len(params) > 0:
                    self.bulk_insert(params, url)
                    self.logger.info('final chunk of dataset field ' + str(len(params)))
                    self.wh_con.commit()

                comment_cursor.close()
                break
            except Exception as e:
                self.logger.error(str(e))
                attempts += 1
                self.logger.error("JDBC error in update dataset fields, retry: {}".format(attempts))
                if self.wh_cursor:
                    self.wh_cursor.close()
                if self.wh_con:
                    self.wh_con.close()
                self.databaseConnect(args)  # reconnect

    def update_comment(self, last_time=None):
        if last_time:
            sql = """
          SELECT * FROM comments WHERE modified >= DATE_SUB(%s, INTERVAL 1 HOUR)
          """ % last_time
        else:
            sql = """
          SELECT * FROM comments
          """

        url = self.elasticsearch_server_url + ':' + str(
            self.elasticsearch_port) + '/' + self.new_index + '/comment/_bulk'
        params = []
        self.wh_cursor.execute(sql)
        row_count = 1
        description = [x[0] for x in self.wh_cursor.description]
        result = self.wh_cursor.fetchone()
        while result:
            row = dict(zip(description, result))
            params.append(
                '{ "index": { "_id": ' + str(row['id']) + ', "parent": ' + str(row['dataset_id']) + '  }}')

            text_detail = {
                'text': row['text'],
                'user_id': row['user_id'],
                'dataset_id': row['dataset_id'],
                'comment_type': row['comment_type']
            }
            params.append(json.dumps(text_detail))

            if row_count % self.bulk_chunk_size == 0:
                self.bulk_insert(params, url)
                self.logger.info('comment ' + str(row_count))
                self.wh_con.commit()
                params = []
            row_count += 1
            result = self.wh_cursor.fetchone()
        if len(params) > 0:
            self.bulk_insert(params, url)
            self.logger.info('comment ' + str(len(params)))
            self.wh_con.commit()

    def update_dataset(self, last_unixtime=None):
        if last_unixtime:
            sql = """
          SELECT d.*,
              COALESCE(s.static_boosting_score,1) as static_boosting_score
          FROM dict_dataset d
          LEFT JOIN cfg_search_score_boost s
          ON d.id = s.id
          WHERE d.urn not like "hive:///dev_foundation_tables%%"
          and d.urn not like "hive:///dev_foundation_views%%"
          and from_unixtime(d.modified_time) >= DATE_SUB(from_unixtime(%f), INTERVAL 1 HOUR)
          """ % last_unixtime
        else:
            sql = """
          INSERT IGNORE INTO cfg_search_score_boost
          (id, static_boosting_score)
          SELECT id, 80 FROM dict_dataset
          WHERE urn like "kafka:///%"
             or urn like "oracle:///%"
             or urn like "espresso:///%"
          ON DUPLICATE KEY UPDATE
          static_boosting_score = 80;


          INSERT IGNORE INTO cfg_search_score_boost
          (id, static_boosting_score)
          SELECT id, 75 FROM dict_dataset
          WHERE urn like "dalids:///%"
          ON DUPLICATE KEY UPDATE
          static_boosting_score = 75;


          INSERT IGNORE INTO cfg_search_score_boost
          (id, static_boosting_score)
          SELECT id, 70 FROM dict_dataset
          WHERE urn like "hdfs:///data/tracking/%"
             or urn like "hdfs:///data/databases/%"
             or urn like "hive:///tracking/%"
             or urn like "hive:///prod_%/%"
          ON DUPLICATE KEY UPDATE
          static_boosting_score = 70;


          INSERT IGNORE INTO cfg_search_score_boost
          (id, static_boosting_score)
          SELECT id, 65 FROM dict_dataset
          WHERE urn like "hdfs:///data/external/%"
             or urn like "hdfs:///data/derived/%"
             or urn like "hdfs:///data/foundation/%"
             or urn like "hive:///hirein/%"
             or urn like "hive:///rightnow/%"
             or urn like "hive:///lla/%"
             or urn like "hive:///append_rightnow/%"
             or urn like "hive:///decipher/%"
             or urn like "hive:///timeforce/%"
             or urn like "hive:///jira/%"
             or urn like "hive:///teleopti/%"
          ON DUPLICATE KEY UPDATE
          static_boosting_score = 65;

          SELECT d.*,
              COALESCE(s.static_boosting_score,1) as static_boosting_score
          FROM dict_dataset d
          LEFT JOIN cfg_search_score_boost s
          ON d.id = s.id
          WHERE d.urn not like "hive:///dev_foundation_tables%"
          and d.urn not like "hive:///dev_foundation_views%"
          """

        self.execute_commands(sql)

        description = [x[0] for x in self.wh_cursor.description]

        row_count = 1
        result = self.wh_cursor.fetchone()

        url = self.elasticsearch_server_url + ':' + str(
            self.elasticsearch_port) + '/' + self.new_index + '/dataset/_bulk'
        params = []
        while result:
            row = dict(zip(description, result))

            name_suggest_info = {
                'input': [row['name']]
            }
            dataset_detail = {
                'name': row['name'],
                'source': row['source'],
                'urn': row['urn'],
                'location_prefix': row['location_prefix'],
                'parent_name': row['parent_name'],
                'schema_type': row['schema_type'],
                'properties': row['properties'],
                'schema': row['schema'],
                'fields': row['fields'],
                'static_boosting_score': row['static_boosting_score'],
                'name_suggest': name_suggest_info
            }

            params.append('{ "index": { "_id": ' + str(row['id']) + ' }}')
            params.append(json.dumps(dataset_detail))


            if row_count % self.bulk_chunk_size  == 0:
                self.bulk_insert(params, url)
                self.logger.info('dataset ' + str(row_count))
                self.wh_con.commit()
                params = []

            row_count += 1
            result = self.wh_cursor.fetchone()
        self.logger.info('total dataset row count is: ' + str(row_count))
        if len(params) > 0:
            self.bulk_insert(params, url)
            self.logger.info('dataset ' + str(len(params)))
            self.wh_con.commit()

    def update_metric(self):
        sql = """
        SELECT * FROM dict_business_metric
        """

        url = self.elasticsearch_server_url + ':' + str(
            self.elasticsearch_port) + '/' + self.new_index + '/metric/_bulk'
        params = []
        self.wh_cursor.execute(sql)
        description = [x[0] for x in self.wh_cursor.description]
        row_count = 1
        result = self.wh_cursor.fetchone()
        while result:
            row = dict(zip(description, result))
            metric_name_suggest_info = {
                'input': [row['metric_name']]
            }
            metric_detail = {
                'metric_id': row['metric_id'],
                'metric_name': row['metric_name'],
                'metric_description': row['metric_description'],
                'dashboard_name': row['dashboard_name'],
                'metric_group': row['metric_group'],
                'metric_category': row['metric_category'],
                'metric_sub_category': row['metric_sub_category'],
                'metric_level': row['metric_level'],
                'metric_source_type': row['metric_source_type'],
                'metric_source': row['metric_source'],
                'metric_source_dataset_id': row['metric_source_dataset_id'],
                'metric_ref_id_type': row['metric_ref_id_type'],
                'metric_ref_id': row['metric_ref_id'],
                'metric_type': row['metric_type'],
                'metric_additive_type': row['metric_additive_type'],
                'metric_grain': row['metric_grain'],
                'metric_display_factor': row['metric_display_factor'],
                'metric_display_factor_sym': row['metric_display_factor_sym'],
                'metric_good_direction': row['metric_good_direction'],
                'metric_formula': row['metric_formula'],
                'dimensions': row['dimensions'],
                'owners': row['owners'],
                'tags': row['tags'],
                'urn': row['urn'],
                'metric_url': row['metric_url'],
                'wiki_url': row['wiki_url'],
                'scm_url': row['scm_url'],
                'metric_name_suggest': metric_name_suggest_info
            }
            params.append('{ "index": { "_id": ' + str(row['metric_id']) + '  }}')
            params.append(json.dumps(metric_detail))

            if row_count % self.bulk_chunk_size == 0:
                self.bulk_insert(params, url)
                self.logger.info('metric ' + str(row_count))
                self.wh_con.commit()
                params = []

            row_count += 1
            result = self.wh_cursor.fetchone()
        if len(params) > 0:
            self.bulk_insert(params, url)
            self.logger.info('metric ' + str(len(params)))
            self.wh_con.commit()

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

        url = self.elasticsearch_server_url + ':' + str(
            self.elasticsearch_port) + '/' + self.new_index + '/flow_jobs/_bulk'

        params = []
        self.wh_cursor.execute(flow_sql)
        job_cursor = self.wh_con.cursor(1)
        description = [x[0] for x in self.wh_cursor.description]
        row_count = 1
        result = self.wh_cursor.fetchone()
        while result:
            row = dict(zip(description, result))
            job_cursor.execute(job_sql % (long(row['app_id']), long(row['flow_id'])))
            jobs_info = []
            job_description = [x[0] for x in job_cursor.description]
            job_result = job_cursor.fetchone()
            while job_result:
                job_row = dict(zip(job_description, job_result))
                flow_name_suggest_info = {
                    'input': [row['flow_name']]
                }
                job_name_suggest_info = {
                    'input': [job_row['job_name']]
                }
                jobs_row_detail = {
                    'app_id': job_row['app_id'],
                    'flow_id': job_row['flow_id'],
                    'job_id': job_row['job_id'],
                    'job_name': job_row['job_name'],
                    'job_path': job_row['job_path'],
                    'job_type_id': job_row['job_type_id'],
                    'job_type': job_row['job_type'],
                    'pre_jobs': job_row['pre_jobs'],
                    'post_jobs': job_row['post_jobs'],
                    'is_current': job_row['is_current'],
                    'is_first': job_row['is_first'],
                    'is_last': job_row['is_last'],
                    'job_name_suggest': job_name_suggest_info
                }
                jobs_info.append(jobs_row_detail)
                job_result = job_cursor.fetchone()

            params.append('{ "index": { "_id": ' + str(long(row['flow_id']) * 10000 + long(row['app_id'])) + '  }}')
            jobs_detail = {
                'app_id': row['app_id'],
                'flow_id': row['flow_id'],
                'app_code': row['app_code'],
                'flow_name': row['flow_name'],
                'flow_group': row['flow_group'],
                'flow_path': row['flow_path'],
                'flow_level': row['flow_level'],
                'is_active': row['is_active'],
                'is_scheduled': row['is_scheduled'],
                'pre_flows': row['pre_flows'],
                'jobs': jobs_info,
                'flow_name_suggest': flow_name_suggest_info
            }
            params.append(json.dumps(jobs_detail))

            if row_count % self.bulk_chunk_size == 0:
                self.bulk_insert(params, url)
                self.logger.info('flow jobs ' + str(row_count))
                self.wh_con.commit()
                params = []

            row_count += 1
            result = self.wh_cursor.fetchone()

        if len(params) > 0:
            self.logger.info('flow_jobs ' + str(len(params)))
            self.bulk_insert(params, url)
            self.wh_con.commit()

        job_cursor.close()

    def execute_commands(self, commands):
        for cmd in commands.split(";"):
            self.logger.info(cmd)
            self.wh_cursor.execute(cmd)
            self.wh_con.commit()

    def es_reindex(self):

        try:
            start_dataset_time = time.time()
            self.update_dataset()
            end_time = time.time()
            self.logger.info('Performance Log update_dataset takes time:  ' + str(end_time - start_dataset_time))

            start_time = time.time()
            self.update_comment()
            end_time = time.time()
            self.logger.info('Performance Log update_comment takes time:  ' + str(end_time - start_time))

            start_time = time.time()
            self.update_dataset_field()
            end_time = time.time()
            self.logger.info('Performance Log update_dataset_field takes time:  ' + str(end_time - start_time))

            start_time = time.time()
            self.update_flow_jobs()
            end_time = time.time()
            self.logger.info('Performance Log update_flow_jobs takes time:  ' + str(end_time - start_time))

            start_time = time.time()
            self.update_metric()
            end_time = time.time()
            self.logger.info('Performance Log update_metric takes time:  ' + str(end_time - start_time))

            self.logger.info(
                'Performance Log ELASTIC_SEARCH_ETL job takes time:  ' + str(time.time() - start_dataset_time))

        except Exception as e:
            self.logger.error(str(e))
            sys.exit("Error in re-indexing, the old index stays active")

        finally:
            self.wh_cursor.close()
            self.wh_con.close()


    def es_http_request(self, method, url, payload):
        try:
            req = urllib2.Request(url=url)
            req.add_header('Content-type', 'application/json')
            req.get_method = lambda: method
            req.add_data(payload)

            self.logger.info("Request sent to ES is: " + url + ' ' + payload)
            response = urllib2.urlopen(req, timeout=self.es_url_request_timeout)
            data = json.load(response)
            return data

        except urllib2.HTTPError as e:
            self.logger.error(str(e.code))
            self.logger.error(e.read())
        except Exception as e:
            self.logger.error(str(e))

    def create_index(self):
        try:
            now = int(time.time())
            url = self.base_url + str(now)

            json_filepath = self.index_mapping_file
            with open(json_filepath, 'r') as f:
                req_body = json.load(f)

            data = self.es_http_request("PUT", url, json.dumps(req_body))
            if str(data['acknowledged']) != 'True':
                self.logger.error(str(data))
                sys.exit("Error in create_index")

            self.new_index = str(now)
            self.logger.info('Successfully created index : {}'.format(self.new_index))

        except Exception as e:
            self.logger.error(str(e))
            sys.exit("Error in create_index")

    def alias_switch(self):
        url = self.base_url + '_aliases'

        remove = '{"remove":{"index": "%s","alias":"%s"}}' % (self.old_index, self.elasticsearch_index)
        add = '{"add":{"index":"%s","alias":"%s"}}' % (self.new_index, self.elasticsearch_index)
        req_body = '{"actions": [%s, %s]}' % (remove, add)

        data = self.es_http_request("POST", url, req_body)

        if str(data['acknowledged']) != 'True':
            self.logger.error(str(data))

        self.logger.info('Successfully switched alias from {} to {}'.format(self.old_index, self.new_index))

    def create_alias(self, index):
        # create a new alias
        url = self.base_url + '_aliases'
        req_body = '{"actions":[{"add":{"index":"%s","alias":"%s"}}]}' %(index, self.elasticsearch_index)
        data = self.es_http_request("POST", url, req_body)

        if str(data['acknowledged']) != 'True':
            self.logger.error(str(data))
        self.logger.info('Successfully create alias for: {}'.format(self.new_index))

    def get_old_index(self):
        # get existing index the current alias points to
        url = self.base_url + self.elasticsearch_index + "/_alias"
        try:
            data = self.es_http_request("GET", url, '')
            for key in data.keys():
                self.old_index = key
                break
            self.logger.info('Successfully find old index: {}'.format(self.old_index))
        except:
            self.logger.info('Need to create alias for the first time')


    def remove_old_index(self):
        url = self.base_url + self.old_index
        data = self.es_http_request("DELETE", url, '')

        if str(data['acknowledged']) != 'True':
            self.logger.error(str(data))
        self.logger.info('Successfully removed index: {}'.format(self.old_index))

    def run(self):
        try:
            # 1, create a new index with current timestamp, using json body from model
            self.create_index()
            # 2, populate data
            self.es_reindex()
            # 3, get old index and remember it
            self.get_old_index()

            if len(self.old_index) > 0:
                # 4, remove existing alias, and add alias to newly created index
                self.alias_switch()
                # 5, remove old timestamp index
                self.remove_old_index()
            else:
                # 6, first time add self.elasticsearch_index alias
                self.create_alias(self.new_index)
        except Exception as e:
            self.logger.error(str(e))


if __name__ == "__main__":
    args = sys.argv[1]
    esi = ElasticSearchIndex(args)
    esi.run()
