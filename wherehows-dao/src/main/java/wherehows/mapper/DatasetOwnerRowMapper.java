/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;
import wherehows.models.DatasetOwner;


public class DatasetOwnerRowMapper implements RowMapper<DatasetOwner> {
  private static final String DATASET_OWNER_ID_COLUMN = "owner_id";
  private static final String DATASET_OWNER_DISPLAY_NAME_COLUMN = "display_name";
  private static final String DATASET_OWNER_EMAIL_COLUMN = "email";
  private static final String DATASET_OWNER_TYPE_COLUMN = "owner_type";
  private static final String DATASET_OWNER_SUB_TYPE_COLUMN = "owner_sub_type";
  private static final String DATASET_OWNER_SORT_ID_COLUMN = "sort_id";
  private static final String DATASET_OWNER_IS_GROUP_COLUMN = "is_group";
  private static final String DATASET_OWNER_ID_TYPE_COLUMN = "owner_id_type";
  private static final String DATASET_OWNER_SOURCE_COLUMN = "owner_source";
  private static final String DATASET_OWNER_NAMESPACE_COLUMN = "namespace";
  private static final String DATASET_OWNER_IS_ACTIVE_COLUMN = "is_active";
  private static final String DATASET_OWNER_CONFIRMED_BY_COLUMN = "confirmed_by";
  private static final String DATASET_OWNER_MODIFIED_TIME_COLUMN = "modified_time";

  @Override
  public DatasetOwner mapRow(ResultSet rs, int rowNum) throws SQLException {
    String userName = rs.getString(DATASET_OWNER_ID_COLUMN);
    String namespace = rs.getString(DATASET_OWNER_NAMESPACE_COLUMN);
    String name = rs.getString(DATASET_OWNER_DISPLAY_NAME_COLUMN);
    String email = rs.getString(DATASET_OWNER_EMAIL_COLUMN);
    String type = rs.getString(DATASET_OWNER_TYPE_COLUMN);
    String subType = rs.getString(DATASET_OWNER_SUB_TYPE_COLUMN);
    String idType = rs.getString(DATASET_OWNER_ID_TYPE_COLUMN);
    String source = rs.getString(DATASET_OWNER_SOURCE_COLUMN);
    Integer sortId = rs.getInt(DATASET_OWNER_SORT_ID_COLUMN);
    Boolean isGroup = "Y".equalsIgnoreCase(rs.getString(DATASET_OWNER_IS_GROUP_COLUMN));
    Boolean isActive = "Y".equalsIgnoreCase(rs.getString(DATASET_OWNER_IS_ACTIVE_COLUMN));
    String confirmedBy = rs.getString(DATASET_OWNER_CONFIRMED_BY_COLUMN);
    Long modifiedTime = rs.getLong(DATASET_OWNER_MODIFIED_TIME_COLUMN);

    DatasetOwner owner = new DatasetOwner();
    owner.userName = userName;
    owner.name = name;
    owner.email = email;
    owner.namespace = namespace;
    owner.sortId = sortId;
    owner.type = type;
    owner.subType = subType;
    owner.idType = idType;
    owner.isGroup = isGroup;
    owner.isActive = isActive;
    owner.source = source;
    owner.confirmedBy = confirmedBy;
    owner.modifiedTime = modifiedTime;

    return owner;
  }
}
