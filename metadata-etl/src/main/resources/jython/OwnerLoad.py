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


class OwnerLoad:
  def __init__(self, args):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    self.wh_cursor = self.wh_con.cursor()
    self.wh_exec_id = long(args[Constant.WH_EXEC_ID_KEY])
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]

  def run(self):
    try:
      cmd = """
            INSERT INTO dataset_owner (dataset_id, dataset_urn, owner_id, sort_id, namespace, app_id, owner_type, owner_sub_type, owner_id_type, owner_source, db_ids, is_group, is_active, source_time, created_time, wh_etl_exec_id)
            SELECT * FROM (SELECT dataset_id, dataset_urn, owner_id, sort_id, namespace, app_id, owner_type, owner_sub_type, owner_id_type, owner_source, group_concat(db_id ORDER BY db_id SEPARATOR ",") db_ids, is_group, is_active, source_time, unix_timestamp(NOW()) time_created, {wh_etl_exec_id}
            FROM stg_dataset_owner s
            WHERE s.dataset_id is not null and s.owner_id is not null and s.owner_id != '' and s.app_id is not null
            GROUP BY s.dataset_id, s.owner_id, s.sort_id, s.namespace, s.owner_type, s.owner_sub_type) sb
            ON DUPLICATE KEY UPDATE
            dataset_urn = sb.dataset_urn,
            sort_id = COALESCE(@sort_id, sb.sort_id),
            owner_type = CASE WHEN sb.owner_type IS NULL OR @owner_type >= sb.owner_type THEN @owner_type ELSE sb.owner_type END,
            owner_sub_type = COALESCE(@owner_sub_type, sb.owner_sub_type),
            owner_id_type = sb.owner_id_type,
            owner_source = CASE WHEN @owner_source IS NULL THEN 'AUDIT'
                            WHEN @owner_source LIKE '%AUDIT%' THEN @owner_source ELSE CONCAT(@owner_source, ',AUDIT') END,
            app_id = sb.app_id,
            is_active = sb.is_active,
            db_ids = sb.db_ids,
            source_time = sb.source_time,
            wh_etl_exec_id = {wh_etl_exec_id},
            modified_time = unix_timestamp(NOW())
            """.format(wh_etl_exec_id=self.wh_exec_id)
      self.logger.debug(cmd)
      self.wh_cursor.execute(cmd)
      self.wh_con.commit()

      # matching parent level urns
      template = """
          INSERT INTO dataset_owner (dataset_id, dataset_urn, owner_id, sort_id, namespace, app_id, owner_type, owner_sub_type, owner_id_type, owner_source, db_ids, is_group, is_active, source_time, created_time, wh_etl_exec_id)
          select * FROM (select distinct d.id, d.urn, s.owner_id, s.sort_id, s.namespace, s.app_id, s.owner_type, owner_sub_type, owner_id_type, owner_source, group_concat(s.db_id ORDER BY db_id SEPARATOR ",") db_ids, s.is_group, s.is_active, s.source_time, unix_timestamp(NOW()) time_created, {wh_etl_exec_id}
          from stg_dataset_owner s join dict_dataset d on s.dataset_urn =  substring(d.urn, 1, char_length(d.urn) - char_length(substring_index(d.urn, '/', -{lvl})) - 1)
          WHERE s.owner_id is not null and s.owner_id != '' and s.app_id is not null
          group by d.id, s.owner_id, s.sort_id, s.namespace, s.owner_type, s.owner_sub_type) sb
          ON DUPLICATE KEY UPDATE
          dataset_urn = sb.urn,
          sort_id = COALESCE(@sort_id, sb.sort_id),
          owner_type = CASE WHEN sb.owner_type IS NULL OR @owner_type >= sb.owner_type THEN @owner_type ELSE sb.owner_type END,
          owner_sub_type = COALESCE(@owner_sub_type, sb.owner_sub_type),
          owner_id_type = sb.owner_id_type,
          owner_source = CASE WHEN @owner_source LIKE '%AUDIT%' THEN @owner_source ELSE CONCAT(@owner_source, ',AUDIT') END,
          app_id = sb.app_id,
          is_active = sb.is_active,
          db_ids = sb.db_ids,
          source_time = sb.source_time,
          wh_etl_exec_id = {wh_etl_exec_id},
          modified_time = unix_timestamp(NOW())
          """

      for l in range(1, 6):
        cmd = template.format(wh_etl_exec_id=self.wh_exec_id, lvl=l)
        self.logger.debug(cmd)
        self.wh_cursor.execute(cmd)
        self.wh_con.commit()

      # put all unmatched dataset in to another table for future reference

      cmd = """
              INSERT INTO stg_dataset_owner_unmatched (dataset_urn, owner_id, sort_id, app_id, namespace, owner_type, owner_sub_type, owner_id_type, owner_source, is_group, db_name, db_id, is_active, source_time)
              SELECT dataset_urn, owner_id, sort_id, app_id, namespace, owner_type, owner_sub_type, owner_id_type, owner_source, is_group, db_name, db_id, is_active, source_time
              FROM stg_dataset_owner s where dataset_id is null and is_parent_urn = 'N'
              ON DUPLICATE KEY UPDATE
              sort_id = s.sort_id,
              owner_type = s.owner_type,
              owner_sub_type = s.owner_sub_type,
              owner_id_type = sb.owner_id_type,
              owner_source = CASE WHEN @owner_source LIKE '%AUDIT%' THEN @owner_source ELSE CONCAT(@owner_source, ',AUDIT') END,
              is_active = s.is_active,
              source_time = s.source_time;
              """
      self.wh_cursor.execute(cmd)
      self.wh_con.commit()

      # delete the entries that matched with dataset id in this round

      cmd = """
              DELETE u FROM stg_dataset_owner_unmatched u
              JOIN (SELECT DISTINCT dataset_urn, dataset_id FROM stg_dataset_owner) s
              ON u.dataset_urn = s.dataset_urn
              WHERE s.dataset_id IS NOT NULL;
              """
      self.wh_cursor.execute(cmd)
      self.wh_con.commit()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()


if __name__ == "__main__":
  props = sys.argv[1]
  ot = OwnerLoad(props)
  ot.run()
