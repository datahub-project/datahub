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

import sys, os, re
import json, csv
import datetime
import xml.etree.ElementTree as ET
from jython import requests
from wherehows.common import Constant
from org.slf4j import LoggerFactory


class MultiproductLoad:

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    requests.packages.urllib3.disable_warnings()
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.multiproduct = {}
    self.git_projects = []
    self.git_repo = {}
    self.product_repo = []
    self.product_repo_owners = []


  def get_multiproducts(self):
    '''
    fetch all products and owners of Multiproduct
    '''
    resp = requests.get(args[Constant.MULTIPRODUCT_SERVICE_URL], verify=False)
    if resp.status_code != 200:
      # This means something went wrong.
      raise Exception('Request Error', 'GET /api/v1/mpl {}'.format(resp.status_code))

    # print resp.content
    re_git_repo_name = re.compile(r":(.*)\.git$")
    re_svn_repo_name = re.compile(r"/(.*)/trunk$")
    if resp.headers['content-type'].split(';')[0] == 'application/json':
      for product_name, product_info in resp.json()['products'].items():
        scm_type = product_info["scm"]["name"]
        try:
          if scm_type == 'git':
            repo_fullname = re_git_repo_name.search(product_info["uris"]["trunk"]).group(1)
            repo_key = 'git:' + repo_fullname
          elif scm_type == 'svn':
            repo_fullname = re_svn_repo_name.search(product_info["uris"]["trunk"]).group(1)
            repo_key = 'svn:' + repo_fullname
        except:
          self.logger.debug("Error parsing repo full name {} - {}".format(product_name, product_info["uris"]))
          continue

        self.multiproduct[repo_key] = {
          "scm_repo_fullname": repo_fullname,
          "scm_type": scm_type,
          "multiproduct_name": product_name,
          "product_type": product_info["type"],
          "namespace": product_info["org"],
          "owner_name": ",".join(product_info["owners"]),
          "product_version": product_info["product-version"]
        }
      self.logger.info("Fetched {} multiproducts".format(len(self.multiproduct)))


  def get_project_repo(self):
    '''
    fetch detail and repos of all git projects
    '''
    re_git_project_name = re.compile(r"(.*)/(.*)$")
    re_git_repo_name = re.compile(r"git://[\w\.-]+/(.*)\.git$")

    project_nonexist = []
    project_names = {}
    for key, product in self.multiproduct.iteritems():
      if product["scm_type"] == 'svn':
        continue

      project_name = re_git_project_name.search(product['scm_repo_fullname']).group(1)
      if project_name in project_names:
        continue
      project_url = '{}/{}?format=xml'.format(args[Constant.GIT_URL_PREFIX], project_name)

      try:
        resp = requests.get(project_url, verify=False)
      except Exception as ex:
        self.logger.info("Error getting /{}.xml - {}".format(project_name, ex.message))
        continue
      if resp.status_code != 200:
        # This means something went wrong.
        self.logger.debug('Request Error: GET /{}.xml {}'.format(project_name, resp.status_code))
        project_nonexist.append(project_name)
        continue

      # print resp.content
      if resp.headers['content-type'].split(';')[0] == 'application/xml':
        xml = ET.fromstring(resp.content)
        current_project = {
          'project_name': xml.find('slug').text,
          'scm_type': 'git',
          'owner_type': xml.find('owner').attrib['kind'],
          'owner_name': xml.find('owner').text,
          'create_time': xml.find('created-at').text,
          'license': xml.find('license').text,
          'description': self.trim_newline(xml.find('description').text)
        }

        project_repo_names = []
        for repo in xml.findall('repositories/mainlines/repository'):
          repo_fullname = re_git_repo_name.search(repo.find('clone_url').text).group(1)
          project_repo_names.append(repo_fullname)
          repo_key = 'git:' + repo_fullname
          self.git_repo[repo_key] = {
            'scm_repo_fullname': repo_fullname,
            'scm_type': 'git',
            'repo_id': repo.find('id').text,
            'project': project_name,
            'owner_type': repo.find('owner').attrib['kind'],
            'owner_name': repo.find('owner').text
          }

        project_repo_num = len(project_repo_names)
        current_project['num_of_repos'] = project_repo_num
        current_project['repos'] = ','.join(project_repo_names)
        self.git_projects.append(current_project)
        project_names[project_name] = project_repo_num
        # self.logger.debug("Project: {} - Repos: {}".format(project_name, project_repo_num))

    csv_columns = ["project_name", "scm_type", "owner_type", "owner_name", "create_time", "num_of_repos", "repos",
                   "license", "description"]
    gitli_project_csv = args[Constant.GIT_PROJECT_OUTPUT_KEY]
    self.write_csv(gitli_project_csv, csv_columns, self.git_projects)

    self.logger.info("Fetched {} projects with {} repos".format(len(self.git_projects), len(self.git_repo)))
    self.logger.debug('Non-exist projects: {}'.format(project_nonexist))


  def merge_product_repo(self):
    '''
    merge multiproduct and repo into same product_repo store
    '''
    for key, repo in self.git_repo.iteritems():
      if key in self.multiproduct:
        mp = self.multiproduct[key]
        repo['multiproduct_name'] = mp["multiproduct_name"]
        repo['product_type'] = mp["product_type"]
        repo['product_version'] = mp["product_version"]
        repo['namespace'] = mp["namespace"]
      self.product_repo.append(repo)

    for key, product in self.multiproduct.iteritems():
      if key not in self.git_repo:
        self.product_repo.append(product)

    csv_columns = ["scm_repo_fullname", "scm_type", "repo_id", "project", "owner_type", "owner_name",
                   "multiproduct_name", "product_type", "product_version", "namespace"]
    gitli_repo_csv = args[Constant.PRODUCT_REPO_OUTPUT_KEY]
    self.write_csv(gitli_repo_csv, csv_columns, self.product_repo)

    self.logger.info("Merged into {} product repos".format(len(self.product_repo)))


  def get_acl_owners(self):
    '''
    fetch owners information from acl
    '''
    re_acl_owners = re.compile(r"owners\:\s*\[([^\[\]]+)\]")
    re_acl_path = re.compile(r"paths\:\s*\[([^\[\]]+)\]")
    re_svn_acl_url = re.compile(r'href=\"[\w\/\-]+[\/\:]acl\/([\w\-\/]+)\.acl(\?revision=\d+)&amp;view=markup\"')
    re_git_acl_url = re.compile(r'href=\"[\w\/\-]+\/source\/([\w\:]*)acl\/([\w\-]+)\.acl\"')

    for repo in self.product_repo:
      if repo['scm_type'] == "git":
        repo_url = '{}/{}/source/acl'.format(args[Constant.GIT_URL_PREFIX], repo['scm_repo_fullname'])
      elif repo['scm_type'] == "svn":
        repo_url = '{}/{}/acl'.format(args[Constant.SVN_URL_PREFIX], repo['scm_repo_fullname'])

      try:
        resp = requests.get(repo_url, verify=False)
      except Exception as ex:
        self.logger.info("Error getting acl {} - {}".format(repo_url, ex.message))
        continue
      if resp.status_code != 200:
        self.logger.debug('Request Error: GET repo {} acls - {}'.format(repo, resp.status_code))
        continue

      if resp.headers['content-type'].split(';')[0] == 'text/html':
        re_acl_url = re_git_acl_url if repo['scm_type'] == "git" else re_svn_acl_url

        for acl_url in re_acl_url.finditer(resp.content):
          if repo['scm_type'] == "git":
            acl_name = acl_url.group(2)
            commit_hash = acl_url.group(1)
            full_acl_url = '{}/{}/raw/{}acl/{}.acl'.format(args[Constant.GIT_URL_PREFIX],
                                                           repo['scm_repo_fullname'], commit_hash, acl_name)
          elif repo['scm_type'] == "svn":
            acl_name = acl_url.group(1)
            commit_hash = acl_url.group(2)
            full_acl_url = '{}/{}.acl{}'.format(repo_url, acl_name, commit_hash)

          try:
            resp = requests.get(full_acl_url, verify=False)
          except Exception as ex:
            self.logger.info("Error getting acl {} - {}".format(full_acl_url, ex.message))
            continue
          if resp.status_code != 200:
            self.logger.debug('Request Error: GET acl {} - {}'.format(full_acl_url, resp.status_code))
            continue

          owners_string = re_acl_owners.search(resp.content)
          path_string = re_acl_path.search(resp.content)
          if owners_string:
            owners = self.parse_owners(owners_string.group(1))
            paths = self.trim_path(path_string.group(1)) if path_string else None
            for owner in owners:
              self.product_repo_owners.append({
                "scm_repo_fullname": repo['scm_repo_fullname'],
                "scm_type": repo['scm_type'],
                "repo_id": repo['repo_id'] if 'repo_id' in repo else 0,
                "owner_type": acl_name,
                "owner_name": owner,
                "paths": paths
              })
            # self.logger.debug('{} acl {} owners: {}'.format(repo['scm_repo_fullname'], acl_name, len(owners)))

    csv_columns = ["scm_repo_fullname", "scm_type", "repo_id", "owner_type", "owner_name", "paths"]
    gitli_repo_owner_csv = args[Constant.PRODUCT_REPO_OWNER_OUTPUT_KEY]
    self.write_csv(gitli_repo_owner_csv, csv_columns, self.product_repo_owners)

    self.logger.info('Fetched {} acl owners for {} repos'.format(len(self.product_repo_owners), len(self.git_repo)))


  def write_csv(self, csv_filename, csv_columns, data_list):
    csvfile = open(csv_filename, 'wb')
    os.chmod(csv_filename, 0644)
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\0')
    writer.writeheader()
    for data in data_list:
      try:
        writer.writerow(data)
      except Exception as ex:
        self.logger.info("Error writing data {} - {}".format(ex.message, str(data)))
    csvfile.close()

  def trim_newline(self, line):
    return line.replace('\n', ' ').replace('\r', ' ').encode('ascii', 'ignore') if line else None

  def trim_path(self, line):
    return line.strip().replace('\n', ' ').replace('\r', ' ').replace('&apos;', "'")

  def parse_owners(self, line):
    elements = [s.strip() for l in line.splitlines() for s in l.split(',')]
    return [x for x in elements if x and not x.startswith('#')]


  def run(self):
    begin = datetime.datetime.now().strftime("%H:%M:%S")
    self.get_multiproducts()
    self.get_project_repo()
    self.merge_product_repo()
    mid = datetime.datetime.now().strftime("%H:%M:%S")
    self.logger.info("Finish getting multiproducts and repos [{} -> {}]".format(str(begin), str(mid)))
    self.get_acl_owners()
    end = datetime.datetime.now().strftime("%H:%M:%S")
    self.logger.info("Extract Multiproduct and gitli metadata [{} -> {}]".format(str(begin), str(end)))


if __name__ == "__main__":
  args = sys.argv[1]

  e = MultiproductLoad()
  e.run()
