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


class OwnerTransform:
  _tables = {"dataset_owner": {"columns": "dataset_urn, owner_id, sort_id, namespace, db_name, source_time",
                               "file": "dataset_owner.csv", "table": "stg_dataset_owner"}}

  _clear_staging_tempalte = """
                            DELETE FROM {table}
                            """

  _read_file_template = """
                        LOAD DATA LOCAL INFILE '{folder}/{file}'
                        INTO TABLE {table}
                        FIELDS TERMINATED BY '\x1a' ESCAPED BY '\0'
                        LINES TERMINATED BY '\n'
                        ({columns})
                        SET owner_source = 'AUDIT';
                        """

  _update_dataset_id_template = """
                          UPDATE {table} stg
                          JOIN dict_dataset dd
                          ON stg.dataset_urn = dd.urn
                          SET stg.dataset_id = dd.id
                          """

  _update_database_id_template = """
                          UPDATE {table} stg
                          JOIN cfg_database cd
                          ON stg.db_name = cd.db_code
                          SET stg.db_id = cd.db_id
                          """

  _update_app_id_template = """
                          UPDATE {table} stg
                          join dir_external_user_info ldap
                          on stg.owner_id = ldap.user_id
                          SET stg.app_id = ldap.app_id,
                          stg.is_group = 'N',
                          stg.owner_id_type = 'user',
                          stg.is_active = ldap.is_active
                          """

  _update_group_app_id_template = """
                          UPDATE {table} stg
                          join dir_external_group_user_map ldap
                          on stg.owner_id = ldap.group_id
                          SET stg.app_id = ldap.app_id,
                          stg.is_group = 'Y',
                          stg.owner_id_type = 'group',
                          stg.is_active = 'Y'
                          """

  _update_owner_type_template = """
                          UPDATE {table} stg
                          join dir_external_user_info ldap
                          on stg.owner_id = ldap.user_id
                          SET stg.owner_type = CASE WHEN ldap.department_id >= 4000 THEN 'Producer' ELSE 'Consumer' END,
                          stg.owner_sub_type = CASE WHEN ldap.department_id = 4020 THEN 'DWH' ELSE 'BA' END
                          """

  _update_parent_flag = """
                          update {table} s
                          join dict_dataset d on s.dataset_urn = substring(d.urn, 1, char_length(d.urn) - char_length(substring_index(d.urn, '/', -{lvl})) - 1)
                          set s.is_parent_urn = 'Y'
                          """

  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.db_id = int(args[Constant.DB_ID_KEY])
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = self.app_folder + "/" + str(self.db_id)

  def run(self):
    try:
      self.read_file_to_stg()
      self.update_dataset_id()
      self.update_database_id()
      self.update_app_id()
      self.update_owner_type()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def read_file_to_stg(self):
    t = self._tables["dataset_owner"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def update_dataset_id(self):
    t = self._tables["dataset_owner"]
    query = self._update_dataset_id_template.format(table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def update_database_id(self):
    t = self._tables["dataset_owner"]
    query = self._update_database_id_template.format(table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def update_app_id(self):
    t = self._tables["dataset_owner"]
    query = self._update_app_id_template.format(table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = self._update_group_app_id_template.format(table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def update_owner_type(self):
    t = self._tables["dataset_owner"]
    query = self._update_owner_type_template.format(table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def update_parent_flag(self):
    t = self._tables["dataset_owner"]
    for l in range(1, 6):
      query = self._update_parent_flag.format(table=t.get("table"), lvl=l)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()


if __name__ == "__main__":
  props = sys.argv[1]
  ot = OwnerTransform(props)
  ot.run()
