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
from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
import sys


class LdapTransform:
  _tables = {"ldap_user": {
    "columns": "app_id, is_active, user_id, urn, full_name, display_name, title, employee_number, manager_urn, email, department_id, department_name, start_date, mobile_phone, wh_etl_exec_id",
    "file": "ldap_user_record.csv",
    "table": "stg_dir_external_user_info",
    "nullif_columns":
      {"department_id": "''",
       "employee_number": 0,
       "start_date": "'0000-00-00'",
       "manager_urn": "''",
       "department_name": "''",
       "mobile_phone": "''",
       "email": "''",
       "title": "''"}
  },
    "ldap_group": {"columns": "app_id, group_id, sort_id, user_app_id, user_id, wh_etl_exec_id",
                   "file": "ldap_group_record.csv",
                   "table": "stg_dir_external_group_user_map",
                   "nullif_columns": {"user_id": "''"}
                   },
    "ldap_group_flatten": {"columns": "app_id, group_id, sort_id, user_app_id, user_id, wh_etl_exec_id",
                           "file": "ldap_group_flatten_record.csv",
                           "table": "stg_dir_external_group_user_map_flatten"
                           }
  }

  _read_file_template = """
                        LOAD DATA LOCAL INFILE '{folder}/{file}'
                        INTO TABLE {table}
                        FIELDS TERMINATED BY '\x1a' ESCAPED BY '\0'
                        LINES TERMINATED BY '\n'
                        ({columns});
                        """

  _update_column_to_null_template = """
                                UPDATE {table} stg
                                SET {column} = NULL
                                WHERE {column} = {column_value} and app_id = {app_id}
                                """

  _update_manager_info = """
                          update {table} stg
                          join (select t1.app_id, t1.user_id, t1.employee_number, t2.user_id as manager_user_id, t2.employee_number as manager_employee_number from
                                {table} t1 join {table} t2 on t1.manager_urn = t2.urn and t1.app_id = t2.app_id
                                where t1.app_id = {app_id}
                          ) s on stg.app_id = s.app_id and stg.user_id = s.user_id
                          set stg.manager_user_id = s.manager_user_id
                            , stg.manager_employee_number = s.manager_employee_number
                          WHERE stg.app_id = {app_id}
                          """

  _get_manager_edge = """
                            select user_id, manager_user_id from {table} stg
                            where app_id = {app_id} and manager_user_id is not null and user_id <> manager_user_id
                            """

  _update_hierarchy_info = """
                            update {table} stg
                            set org_hierarchy = CASE {org_hierarchy_long_string} END,
                                org_hierarchy_depth = CASE {org_hierarchy_depth_long_string} END
                            where app_id = {app_id} and user_id in ({user_ids})
                            """

  _update_hierarchy_info_per_row = """
                            update {table} stg
                            set org_hierarchy = '{org_hierarchy}',
                                org_hierarchy_depth = {org_hierarchy_depth}
                            where app_id = {app_id} and user_id = '{user_id}'
                            """

  _clear_staging_tempalte = """
                            DELETE FROM {table} where app_id = {app_id}
                            """

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.group_app_id = int(args[Constant.LDAP_GROUP_APP_ID_KEY])
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(self.app_id)
    self.ceo_user_id = args[Constant.LDAP_CEO_USER_ID_KEY]

  def run(self):
    try:
      self.read_file_to_stg()
      self.update_null_value()
      self.update_manager_info()
      self.update_hierarchy_info()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def read_file_to_stg(self):

    for table in self._tables:
      t = self._tables[table]
      # Clear stagging table
      query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
      print query
      self.wh_cursor.execute(query)
      self.wh_con.commit()

      # Load file into stagging table
      query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"), columns=t.get("columns"))
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

  def update_null_value(self):
    for table in self._tables:
      t = self._tables[table]
      if 'nullif_columns' in t:
        for column in t['nullif_columns']:
          query = self._update_column_to_null_template.format(table=t.get("table"), column=column, column_value=t['nullif_columns'][column], app_id=self.app_id)
          self.logger.debug(query)
          self.wh_cursor.execute(query)
          self.wh_con.commit()

  def update_manager_info(self):
    t = self._tables["ldap_user"]
    query = self._update_manager_info.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def update_hierarchy_info(self):
    t = self._tables["ldap_user"]
    query = self._get_manager_edge.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    user_mgr_map = dict()
    hierarchy = dict()

    for row in self.wh_cursor:
      user_mgr_map[row[0]] = row[1]

    for user in user_mgr_map:
      self.find_path_for_user(user, user_mgr_map, hierarchy)

    case_org_hierarchy_template = " WHEN user_id = '{user_id}' THEN '{org_hierarchy}' "
    case_org_hierarchy_depth_template = " WHEN user_id = '{user_id}' THEN {org_hierarchy_depth} "
    user_ids = []
    org_hierarchy_long_string = ""
    org_hierarchy_depth_long_string = ""
    count = 0
    for user in hierarchy:
      if hierarchy[user] is not None:
        user_ids.append("'" + user + "'")
        org_hierarchy_long_string += case_org_hierarchy_template.format(user_id=user, org_hierarchy=hierarchy[user][0])
        org_hierarchy_depth_long_string += case_org_hierarchy_depth_template.format(user_id=user, org_hierarchy_depth=hierarchy[user][1])
        count += 1
        if count % 1000 == 0:
          query = self._update_hierarchy_info.format(table=t.get("table"), app_id=self.app_id, user_ids=",".join(user_ids), org_hierarchy_long_string=org_hierarchy_long_string,
                                                     org_hierarchy_depth_long_string=org_hierarchy_depth_long_string)
          # self.logger.debug(query)
          self.wh_cursor.executemany(query)
          user_ids = []
          org_hierarchy_long_string = ""
          org_hierarchy_depth_long_string = ""

    query = self._update_hierarchy_info.format(table=t.get("table"), app_id=self.app_id, user_ids=",".join(user_ids), org_hierarchy_long_string=org_hierarchy_long_string,
                                               org_hierarchy_depth_long_string=org_hierarchy_depth_long_string)
    # self.logger.debug(query)
    self.wh_cursor.executemany(query)
    self.wh_con.commit()

  def find_path_for_user(self, start, user_mgr_map, hierarchy):
    if start in hierarchy:
      return hierarchy[start]

    if start is None or start == '':
      return None

    path = "/" + start
    depth = 0
    user = start
    while user in user_mgr_map:
      if user == self.ceo_user_id or user == user_mgr_map[user]:
        break
      user = user_mgr_map[user]
      path = "/" + user + path
      depth += 1
      if user == self.ceo_user_id:
        break

    if path:
      hierarchy[start] = (path, depth)
    if len(hierarchy) % 1000 == 0:
      self.logger.info("%d hierarchy path created in cache so far. [%s]" % (len(hierarchy), start))

    return (path, depth)


if __name__ == "__main__":
  props = sys.argv[1]
  lt = LdapTransform(props)
  lt.run()
