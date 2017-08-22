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

    owner.setUserName(rs.getString("owner_id"));
    owner.setName(rs.getString("display_name"));
    owner.setNamespace(rs.getString("namespace"));
    owner.setEmail(rs.getString("email"));
    owner.setType(rs.getString("owner_type"));
    owner.setSubType(rs.getString("owner_sub_type"));
    owner.setIdType(rs.getString("owner_id_type"));
    owner.setSource(rs.getString("owner_source"));
    owner.setIsGroup("Y".equalsIgnoreCase(rs.getString("is_group")));
    owner.setIsActive("Y".equalsIgnoreCase(rs.getString("is_active")));
    owner.setSortId(rs.getInt("sort_id"));
    owner.setConfirmedBy(rs.getString("confirmed_by"));
    owner.setModifiedTime(rs.getLong("modified_time") * 1000);

    return owner;
  }
}
