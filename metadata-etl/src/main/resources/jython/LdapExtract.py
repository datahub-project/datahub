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

from org.slf4j import LoggerFactory
from javax.naming.directory import InitialDirContext
from javax.naming.ldap import InitialLdapContext
from javax.naming import Context
from javax.naming.directory import SearchControls
from javax.naming.directory import BasicAttributes
from javax.naming.ldap import Control
from javax.naming.ldap import PagedResultsControl
from javax.naming.ldap import PagedResultsResponseControl
from wherehows.common import Constant

import csv, re, os, sys, json
from java.util import Hashtable
from jarray import zeros, array
from java.io import FileWriter


class LdapExtract:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.args = args
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.group_app_id = int(args[Constant.LDAP_GROUP_APP_ID_KEY])
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(self.app_id)
    if not os.path.exists(self.metadata_folder):
      try:
        os.makedirs(self.metadata_folder)
      except Exception as e:
        self.logger.error(e)

    self.ldap_user = set()
    self.group_map = dict()
    self.group_flatten_map = dict()

  def fetch_ldap_user(self, file):
    """
    fetch ldap user from ldap server
    :param file: output file name
    """

    # Setup LDAP Context Options
    settings = Hashtable()
    settings.put(Context.INITIAL_CONTEXT_FACTORY, self.args[Constant.LDAP_CONTEXT_FACTORY_KEY])
    settings.put(Context.PROVIDER_URL, self.args[Constant.LDAP_CONTEXT_PROVIDER_URL_KEY])
    settings.put(Context.SECURITY_PRINCIPAL, self.args[Constant.LDAP_CONTEXT_SECURITY_PRINCIPAL_KEY])
    settings.put(Context.SECURITY_CREDENTIALS, self.args[Constant.LDAP_CONTEXT_SECURITY_CREDENTIALS_KEY])

    # page the result, each page have fix number of records
    pageSize = 5000
    pageControl = PagedResultsControl(pageSize, Control.NONCRITICAL)
    c_array = array([pageControl], Control)

    # Connect to LDAP Server
    ctx = InitialLdapContext(settings, None)
    ctx.setRequestControls(c_array);

    # load the java Hashtable out of the ldap server
    # Query starting point and query target
    search_target = '(objectClass=person)'
    return_attributes_standard = ['user_id', 'distinct_name', 'name', 'display_name', 'title', 'employee_number',
                                  'manager', 'mail', 'department_number', 'department', 'start_date', 'mobile']
    return_attributes_actual = json.loads(self.args[Constant.LDAP_SEARCH_RETURN_ATTRS_KEY])
    return_attributes_map = dict(zip(return_attributes_standard, return_attributes_actual))

    ctls = SearchControls()
    ctls.setReturningAttributes(return_attributes_actual)
    ctls.setSearchScope(SearchControls.SUBTREE_SCOPE)
    ldap_records = []

    # domain format should look like : ['OU=domain1','OU=domain2','OU=domain3,OU=subdomain3']
    org_units = json.loads(self.args[Constant.LDAP_SEARCH_DOMAINS_KEY])

    cookie = None
    for search_unit in org_units:
      # pagination
      while True:
        # do the search
        search_result = ctx.search(search_unit, search_target, ctls)
        for person in search_result:
          ldap_user_tuple = [self.app_id]
          if search_unit == self.args[Constant.LDAP_INACTIVE_DOMAIN_KEY]:
            ldap_user_tuple.append('N')
          else:
            ldap_user_tuple.append('Y')
          person_attributes = person.getAttributes()
          user_id = person_attributes.get(return_attributes_map['user_id'])
          user_id = re.sub(r"\r|\n", '', user_id.get(0)).strip().encode('utf8')
          self.ldap_user.add(user_id)

          for attr_name in return_attributes_actual:
            attr = person_attributes.get(attr_name)
            if attr:
              attr = re.sub(r"\r|\n", '', attr.get(0)).strip().encode('utf8')
              # special fix for start_date
              if attr_name == return_attributes_map['start_date'] and len(attr) == 4:
                attr += '0101'
              ldap_user_tuple.append(attr)
            else:
              ldap_user_tuple.append("")

          ldap_user_tuple.append(self.wh_exec_id)
          ldap_records.append(ldap_user_tuple)

        # Examine the paged results control response
        control = ctx.getResponseControls()[0] # will always return a list, but only have one item
        if isinstance(control, PagedResultsResponseControl):
          cookie = control.getCookie()

        # Re-activate paged results
        if cookie is None:
          # reset ctx, break while loop, do next search
          pageControl = PagedResultsControl(pageSize, Control.NONCRITICAL)
          c_array = array([pageControl], Control)
          ctx.setRequestControls(c_array)
          break
        else:
          self.logger.debug("Have more than one page of result when search " + search_unit)
          pageControl = PagedResultsControl(pageSize, cookie, Control.CRITICAL)
          c_array = array([pageControl], Control)
          ctx.setRequestControls(c_array)

    self.logger.info("%d records found in ldap search" % (len(self.ldap_user)))

    csv_writer = csv.writer(open(file, "w"), delimiter='\x1a', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    csv_writer.writerows(ldap_records)

  def fetch_ldap_group(self, file):
    """
    fetch group mapping from group ldap server
    :param file: output file name
    """
    settings = Hashtable()
    settings.put(Context.INITIAL_CONTEXT_FACTORY, self.args[Constant.LDAP_GROUP_CONTEXT_FACTORY_KEY])
    settings.put(Context.PROVIDER_URL, self.args[Constant.LDAP_GROUP_CONTEXT_PROVIDER_URL_KEY])
    settings.put(Context.SECURITY_PRINCIPAL, self.args[Constant.LDAP_GROUP_CONTEXT_SECURITY_PRINCIPAL_KEY])
    settings.put(Context.SECURITY_CREDENTIALS, self.args[Constant.LDAP_GROUP_CONTEXT_SECURITY_CREDENTIALS_KEY])

    ctx = InitialDirContext(settings)
    search_target = "(objectClass=posixGroup)"
    return_attributes_standard = ['group_id', 'member_ids']
    return_attributes_actual = json.loads(self.args[Constant.LDAP_GROUP_SEARCH_RETURN_ATTRS_KEY])
    return_attributes_map = dict(zip(return_attributes_standard, return_attributes_actual))
    ctls = SearchControls()
    ctls.setReturningAttributes(return_attributes_actual)
    ctls.setSearchScope(SearchControls.SUBTREE_SCOPE)

    ldap_records = []
    org_units = json.loads(self.args[Constant.LDAP_GROUP_SEARCH_DOMAINS_KEY])
    for search_unit in org_units:
      results = ctx.search(search_unit, search_target, ctls)
      for r in results:
        person_attributes = r.getAttributes()
        group = person_attributes.get(return_attributes_map['group_id']).get(0)
        group = re.sub(r"\r|\n", '', group).strip().encode('utf8')
        # skip special group that contains all group users
        if group == 'users':
          continue
        members = person_attributes.get(return_attributes_map['member_ids'])
        if members:
          self.group_map[group] = members
          sort_id = 0
          for member in members.getAll():
            member = re.sub(r"\r|\n", '', member).strip().encode('utf8')
            ldap_group_tuple = [self.group_app_id]
            ldap_group_tuple.append(group)
            ldap_group_tuple.append(sort_id)
            if member in self.ldap_user:
              ldap_group_tuple.append(self.app_id)
            else:
              ldap_group_tuple.append(self.group_app_id)
            ldap_group_tuple.append(member)
            ldap_group_tuple.append(self.wh_exec_id)
            ldap_records.append(ldap_group_tuple)
            sort_id += 1
        else:
          pass
    self.logger.info("%d records found in group accounts" % (len(self.group_map)))

    csv_writer = csv.writer(open(file, "w"), delimiter='\x1a', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    csv_writer.writerows(ldap_records)

  def fetch_ldap_group_flatten(self, file):
    """
    Flatten the group - user map by recursive extending inner-group members
    :param file: output file name
    """
    ldap_records = []
    for group in self.group_map:
      all_users = self.get_all_users_for_group(group, self.ldap_user, self.group_map, set())
      self.group_flatten_map[group] = all_users
      sort_id = 0
      for u in all_users:
        ldap_group_flatten_tuple = [self.group_app_id]
        ldap_group_flatten_tuple.append(group)
        ldap_group_flatten_tuple.append(sort_id)
        ldap_group_flatten_tuple.append(self.app_id)
        ldap_group_flatten_tuple.append(u)
        ldap_group_flatten_tuple.append(self.wh_exec_id)
        ldap_records.append(ldap_group_flatten_tuple)
        sort_id += 1

    csv_writer = csv.writer(open(file, "w"), delimiter='', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    csv_writer.writerows(ldap_records)

  def get_all_users_for_group(self, current, user_set, group_map, previous):
    """
    Recursive method that calculate all users for current group
    :param current: current group name
    :param user_set: the user set that contains all user ids
    :param group_map: the original group user map before extend
    :param previous: previous visited group name
    :return: ordered list of users
    """
    ret = []
    # base condition
    if current in user_set:
      ret.append(current)
      return ret

    # cyclic condition
    if current in previous:
      return ret

    # avoid duplicate computation
    if current in self.group_flatten_map:
      return self.group_flatten_map[current]

    # current is a group
    if current in group_map:
      members = group_map[current]
      previous.add(current)
      for member in members.getAll():
        member = re.sub(r"\r|\n", '', member).strip().encode('utf8')
        next_ret = self.get_all_users_for_group(member, user_set, group_map, previous)
        for i in next_ret:
          if i not in ret:
            ret.append(i)
    return ret

  def run(self):
    self.fetch_ldap_user(self.metadata_folder + "/ldap_user_record.csv")
    self.fetch_ldap_group(self.metadata_folder + "/ldap_group_record.csv")
    self.fetch_ldap_group_flatten(self.metadata_folder + "/ldap_group_flatten_record.csv")


if __name__ == "__main__":
  props = sys.argv[1]
  ldap = LdapExtract(props)
  ldap.run()
