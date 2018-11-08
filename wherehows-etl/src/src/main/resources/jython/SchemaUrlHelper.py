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
    :param keytab_file: optional, absolute path to keytab file
    """
    self.logger = LoggerFactory.getLogger(self.__class__.__name__)

    self.logger.info("keytab_file: " + keytab_file)

    hdfs_conf = Configuration()
    if hdfs_uri.startswith('hdfs://'):
      hdfs_conf.set(Hdfs.FS_DEFAULT_NAME_KEY, hdfs_uri)
    elif hdfs_uri > "":
      self.logger.error("%s is an invalid uri for hdfs namenode ipc bind." % hdfs_uri)

    if kerberos:  #  init kerberos and keytab
      if not kerberos_principal or not keytab_file or kerberos_principal == '' or keytab_file == '':
        print "Kerberos Principal and Keytab File Name/Path are required!"

      hdfs_conf.set("hadoop.security.authentication", "kerberos")
      hdfs_conf.set("dfs.namenode.kerberos.principal.pattern", "*")
      UserGroupInformation.setConfiguration(hdfs_conf)
      UserGroupInformation.loginUserFromKeytab(kerberos_principal, keytab_file)

    self.fs = Hdfs.get(hdfs_conf)

    requests.packages.urllib3.disable_warnings()
    self.logger.info("Initiated SchemaUrlHelper")


  def get_from_hdfs(self, file_loc):
    """
    Try to get the text content from HDFS based on file_loc
    Return schema literal string
    """
    fp = HdfsPath(file_loc)
    try:
      if self.fs.exists(fp):
        in_stream = self.fs.open(fp)
        self.logger.info('GET schema literal from {}'.format(file_loc))
        return IOUtils.toString(in_stream, 'UTF-8')
      else:
        self.logger.info('Schema not exists: {}'.format(file_loc))
        return None
    except Exception as e:
      self.logger.error(str(e))
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

    if resp.status_code == 200 and 'content-length' in resp.headers:
      self.logger.info('GET {} bytes from {}'.format(resp.headers['content-length'], file_loc))
      return resp.text
    else:
      # This means something went wrong.
      self.logger.error('Request Error for {}: {}\n Header: {}'.format(file_loc, resp.status_code, resp.headers))
      return None
