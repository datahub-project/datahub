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
  @Override
  public DatasetOwner mapRow(ResultSet rs, int rowNum) throws SQLException {
    final DatasetOwner owner = new DatasetOwner();

    owner.userName = rs.getString("owner_id");
    owner.name = rs.getString("display_name");
    owner.namespace = rs.getString("namespace");
    owner.email = rs.getString("email");
    owner.type = rs.getString("owner_type");
    owner.subType = rs.getString("owner_sub_type");
    owner.idType = rs.getString("owner_id_type");
    owner.source = rs.getString("owner_source");
    owner.isGroup = "Y".equalsIgnoreCase(rs.getString("is_group"));
    owner.isActive = "Y".equalsIgnoreCase(rs.getString("is_active"));
    owner.sortId = rs.getInt("sort_id");
    owner.confirmedBy = rs.getString("confirmed_by");
    owner.modifiedTime = rs.getLong("modified_time") * 1000;

    return owner;
  }
}
