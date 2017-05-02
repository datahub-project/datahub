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
package dao;

import com.fasterxml.jackson.databind.JsonNode;
import models.SchemaDataset;
import models.SchemaHistoryData;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;
import play.Logger;
import play.libs.Json;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Time;

public class SchemaHistoryDataRowMapper implements RowMapper<SchemaHistoryData>
{
    @Override
    public SchemaHistoryData mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        String modified = rs.getString("modified_date");
        String schema = rs.getString("schema");
        int fieldCount = 0;
        if (StringUtils.isNotBlank(schema))
        {
            try {
                JsonNode schemaNode = Json.parse(schema);
                fieldCount =  utils.SchemaHistory.calculateFieldCount(schemaNode);
            } catch (Exception e) {
                Logger.error("SchemaHistoryDataRowMapper get field count parse schema failed");
                Logger.error("Exception = " + e.getMessage());
            }
        }
        SchemaHistoryData schemaHistoryData = new SchemaHistoryData();
        schemaHistoryData.modified = modified;
        schemaHistoryData.schema = schema;
        schemaHistoryData.fieldCount = fieldCount;
        return schemaHistoryData;
    }
}