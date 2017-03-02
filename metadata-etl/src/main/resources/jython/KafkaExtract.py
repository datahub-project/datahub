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

import sys, json, re
from datetime import datetime
from jython import requests
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class KafkaExtract:

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    requests.packages.urllib3.disable_warnings()

    self.output_file = open(args[Constant.KAFKA_OUTPUT_KEY], 'w')

    self.d2_proxys = []
    proxy_urls = [x.strip() for x in args[Constant.D2_PROXY_URL].split(',')]
    for url in proxy_urls:
      start = datetime.now()
      resp = requests.get(url, verify=False)
      elapsed = datetime.now() - start
      self.d2_proxys.append({'proxy': url, 'latency': elapsed})
      self.logger.debug("url:{}, latency:{}".format(url, elapsed))

    self.d2_proxys = sorted(self.d2_proxys, key = lambda item: (item['latency']))
    self.d2_proxy_url = self.d2_proxys[0]['proxy']
    self.logger.info("Using proxy: {}".format(self.d2_proxy_url))


  def get_nuage_kafka_metadata(self):
    '''
    get KAFKA metadata from nuage
    '''
    headers = {'Accept': 'application/json'}
    payload = {'q': 'type', 'type': 'KAFKA', 'subType': 'KAFKA_TRACKING', 'fields': 'subType,fabric,name'}
    resp = requests.get(self.d2_proxy_url + '/nuageDatabases', params=payload, headers=headers, verify=False)

    if resp.status_code != 200:
      self.logger.error(resp.text)
    all_tables = resp.json()

    # merge the same name to one
    merged_all_tables = {}    # {name : {'fabrics':[], subType:''}}
    for one_table in all_tables['elements']:
      name = one_table['name']
      fabric = one_table['fabric']
      subType = one_table['subType'] if 'subType' in one_table else None
      if name in merged_all_tables:
        merged_all_tables[name]['fabrics'].append(fabric)
      else:
        merged_all_tables[name] = {'fabrics': [fabric], 'subType': subType}

    self.logger.info("Found {} topics for KAFKA TRACKING".format(len(merged_all_tables)))

    skip_pattern_startswith = re.compile("^(_|test|tmp).*$")
    skip_pattern_endswith = re.compile("^.*(test|testing|tmp)\d*$")

    table_count = 0
    for name, value in merged_all_tables.items():
      if skip_pattern_startswith.match(name) or skip_pattern_endswith.match(name):
        continue
      if 'PROD' in value['fabrics']:
        fabric = 'PROD'
      elif 'CORP' in value['fabrics']:
        fabric = 'CORP'
      else:
        continue

      sub_type = value['subType'] if value['subType'] > '' else ''

      req_params = 'NuageDatabaseName={}&fabric={}&type={}&subType={}'.format(name, fabric, 'KAFKA', sub_type)
      resp = requests.get(self.d2_proxy_url + '/nuageDatabases/' + req_params, headers=headers, verify=False)
      if resp.status_code != 200:
        self.logger.info('Request ERROR {}: {}'.format(resp.status_code, req_params))
        continue
      else:
        one_table_info = resp.json()

      if one_table_info is not None:
        one_table_info['fabrics'] = value['fabrics']
        self.output_file.write(json.dumps(one_table_info))
        self.output_file.write('\n')
        table_count += 1
        self.logger.info("{} : {}".format(table_count, name))
    self.output_file.close()
    self.logger.info('Extracted {} topics for KAFKA'.format(table_count))


  def run(self):
    begin = datetime.now().strftime("%H:%M:%S")
    self.get_nuage_kafka_metadata()
    end = datetime.now().strftime("%H:%M:%S")
    self.logger.info("Extract KAFKA metadata from Nuage [{} -> {}]".format(str(begin), str(end)))


if __name__ == "__main__":
  args = sys.argv[1]

  e = KafkaExtract()
  e.run()
