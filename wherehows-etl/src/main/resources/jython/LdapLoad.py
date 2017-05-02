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


class LdapLoad:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.app_id = int(args[Constant.APP_ID_KEY])
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(self.app_id)

  def run(self):
    try:
      self.load_from_stg()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def load_from_stg(self):
    query = """
        INSERT INTO dir_external_user_info
        (
          app_id, user_id, urn, full_name, display_name, title, employee_number,
          manager_urn, manager_user_id, manager_employee_number, default_group_name, email, department_id, department_name, start_date, mobile_phone,
          is_active, org_hierarchy, org_hierarchy_depth, created_time, wh_etl_exec_id
        )
        select app_id, user_id, urn, full_name, display_name, title, employee_number,
          manager_urn, manager_user_id, manager_employee_number, default_group_name, email, department_id, department_name, start_date, mobile_phone,
          is_active, org_hierarchy, org_hierarchy_depth, unix_timestamp(NOW()), wh_etl_exec_id
        from stg_dir_external_user_info s
        on duplicate key update
          urn = s.urn,
          full_name = s.full_name,
          display_name = trim(s.display_name),
          title = trim(s.title),
          employee_number = coalesce(s.employee_number, @employee_number),
          manager_urn = s.manager_urn,
          manager_user_id = s.manager_user_id,
          manager_employee_number = s.manager_employee_number,
          default_group_name = s.default_group_name,
          email = s.email,
          department_id = coalesce(s.department_id, @department_id),
          department_name = coalesce(trim(s.department_name), @department_name),
          start_date = s.start_date,
          mobile_phone = trim(s.mobile_phone),
          is_active = s.is_active,
          org_hierarchy = coalesce(s.org_hierarchy, @org_hierarchy),
          org_hierarchy_depth = coalesce(s.org_hierarchy_depth, @org_hierarchy_depth),
          modified_time = unix_timestamp(NOW()),
          wh_etl_exec_id = s.wh_etl_exec_id
        """
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
        INSERT INTO dir_external_group_user_map
        (app_id, group_id, sort_id, user_app_id, user_id, created_time, wh_etl_exec_id)
        SELECT app_id, group_id, sort_id, user_app_id, user_id, unix_timestamp(NOW()), wh_etl_exec_id
        FROM stg_dir_external_group_user_map s
        ON DUPLICATE KEY UPDATE
        modified_time = unix_timestamp(NOW()),
        wh_etl_exec_id = s.wh_etl_exec_id
        """
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
        INSERT INTO dir_external_group_user_map_flatten
        (app_id, group_id, sort_id, user_app_id, user_id, created_time, wh_etl_exec_id)
        SELECT app_id, group_id, sort_id, user_app_id, user_id, unix_timestamp(NOW()), wh_etl_exec_id
        FROM stg_dir_external_group_user_map_flatten s
        ON DUPLICATE KEY UPDATE
        modified_time = unix_timestamp(NOW()),
        wh_etl_exec_id = s.wh_etl_exec_id
        """
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()


if __name__ == "__main__":
  props = sys.argv[1]
  lt = LdapLoad(props)
  lt.run()
