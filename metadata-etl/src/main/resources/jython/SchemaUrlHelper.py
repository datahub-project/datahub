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

import sys, os
import json
from os.path import expanduser

from java.util import Hashtable
from java.io import InputStreamReader
from java.util import Properties
from java.util import Date

from org.apache.commons.io import IOUtils
from org.apache.hadoop.conf import Configuration
from org.apache.hadoop.fs import FileSystem as Hdfs
from org.apache.hadoop.fs import Path as HdfsPath
from org.apache.hadoop.security import UserGroupInformation

from jython import requests
from org.slf4j import LoggerFactory


class SchemaUrlHelper:
  """
  This class fetches schema literal from avro.schema.url
  It supports:
    * http://schema-registry-server:port/schemaRegistry/schemas/latest_with_type=SearchImpressionEvent
    * hdfs://hadoop-name-node:port/data/external/Customer/Profile/daily/2016/01/01/_schema.avsc
    * /data/databases/ADS/CAMPAIGNS/snapshot/v2.3/_schema.avsc
  """

  def __init__(self, hdfs_uri, kerberos=False, kerberos_principal=None, keytab_file=None):
    """
    :param hdfs_uri: hdfs://hadoop-name-node:port
    :param kerberos: optional, if kerberos authentication is needed
    :param kerberos_principal: optional, user@DOMAIN.COM
    :param keytab_file: optional, user.keytab or ~/.kerberos/user.keytab
    """

    self.logger = LoggerFactory.getLogger(self.__class__.__name__)

    hdfs_conf = Configuration()
    if hdfs_uri.startswith('hdfs://'):
      hdfs_conf.set(Hdfs.FS_DEFAULT_NAME_KEY, hdfs_uri)
    elif hdfs_uri > "":
      self.logger.error("%s is an invalid uri for hdfs namenode ipc bind." % hdfs_uri)

    if kerberos == True:  #  init kerberos and keytab
      if not kerberos_principal or not keytab_file or kerberos_principal == '' or keytab_file == '':
        print "Kerberos Principal and Keytab File Name/Path are required!"

      keytab_path = keytab_file
      if keytab_file.startswith('/'):
        if os.path.exists(keytab_file):
          keytab_path = keytab_file
          print "Using keytab at %s" % keytab_path
      else:  # try relative path
        all_locations = [os.getcwd(), expanduser("~") + "/.ssh",
            expanduser("~") + "/.kerberos", expanduser("~") + "/.wherehows",
            os.getenv("APP_HOME"), os.getenv("WH_HOME")]
        for loc in all_locations:
          if os.path.exists(loc + '/' + keytab_file):
            keytab_path = loc + '/' + keytab_file
            print "Using keytab at %s" % keytab_path
            break

      hdfs_conf.set("hadoop.security.authentication", "kerberos")
      hdfs_conf.set("dfs.namenode.kerberos.principal.pattern", "*")
      UserGroupInformation.setConfiguration(hdfs_conf)
      UserGroupInformation.loginUserFromKeytab(kerberos_principal, keytab_path)

    self.fs = Hdfs.get(hdfs_conf)

    requests.packages.urllib3.disable_warnings()

  def get_from_hdfs(self, file_loc):
    """
    Try to get the text content from HDFS based on file_loc
    Return schema literal string
    """

    fp = HdfsPath(file_loc)
    try:
      if self.fs.exists(fp):
        in_stream = self.fs.open(fp)
        return IOUtils.toString(in_stream, 'UTF-8')
      else:
        return None
    except:
      return None    

  def get_from_http(self, file_loc):
    """
    Try to get the schema from HTTP/HTTPS based on file_loc
    """
    try:
      resp = requests.get(file_loc, verify=False)
    except Exception as e:
      self.logger.error(str(e))
      return None

    if resp.status_code == 200:
      if 'content-length' in resp.headers:
        self.logger.debug('GET {} bytes from {}'.format(resp.headers['content-length'], file_loc))
      return resp.text
    else:
      # This means something went wrong.
      raise Exception('Request Error', 'GET {} {}'.format(file_loc, resp.status_code))
      return None




if __name__ == "__main__":
  HTTP_TEST_URI = ['http://ltx1-schema-registry-vip-1.prod.linkedin.com:12250/schemaRegistry/schemas/latest_with_type=NativeRealUserMonitoringEvent',
                   'http://lva1-schema-registry-vip-2.corp.linkedin.com:12250/schemaRegistry/schemas/latest_with_type=MessageDeliveryEvent',
                   'https://ipinfo.io/', 'https://api.github.com/users/linkedin']
  HDFS_TEST_URI = ['hdfs://ltx1-unonn01.grid.linkedin.com:9000/data/dbchanges/Identity/Profile/daily/2016/09/29/_schema.avsc',
                   '/data/dbchanges/Identity/Profile/daily/2016/10/01/_schema.avsc']

  schema_url_helper = SchemaUrlHelper("hdfs://ltx1-unonn01.grid.linkedin.com:9000", kerberos=True, \
                                      kerberos_principal="wherehow@GRID.LINKEDIN.COM", \
                                      keytab_file="wherehow.headless.keytab")

  for f in HDFS_TEST_URI:
    print "Test HDFS:"
    if f.startswith('hdfs://') or f.startswith('webhdfs://') or f.startswith('/'):
      print "    Schema URL = %s" % f
      print "Schema Literal = %s" % schema_url_helper.get_from_hdfs(f)

  for f in HTTP_TEST_URI:
    print "Test HTTP:"
    if f.startswith('https://') or f.startswith('http://'):
      print "    Schema URL = %s" % f
      print "Schema Literal = %s" % schema_url_helper.get_from_http(f)
