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
package controllers.api.v1;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dao.SchemaHistoryDAO;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import org.apache.commons.lang3.StringUtils;

public class SchemaHistory extends Controller
{

    public static Result getPagedDatasets()
    {
        ObjectNode result = Json.newObject();

        String name = request().getQueryString("name");
        int page = 1;
        String pageStr = request().getQueryString("page");
        if (StringUtils.isBlank(pageStr))
        {
            page = 1;
        }
        else
        {
            try
            {
                page = Integer.parseInt(pageStr);
            }
            catch(NumberFormatException e)
            {
                Logger.error("SchemaHistory Controller getPagedDatasets wrong page parameter. Error message: " +
                        e.getMessage());
                page = 1;
            }
        }

        Long datasetId = 0L;
        String datasetIdStr = request().getQueryString("datasetId");
        if (StringUtils.isNotBlank(datasetIdStr))
        {
            try
            {
                datasetId = Long.parseLong(datasetIdStr);
            }
            catch(NumberFormatException e)
            {
                datasetId = 0L;
            }
        }

        int size = 10;
        String sizeStr = request().getQueryString("size");
        if (StringUtils.isBlank(sizeStr))
        {
            size = 10;
        }
        else
        {
            try
            {
                size = Integer.parseInt(sizeStr);
            }
            catch(NumberFormatException e)
            {
                Logger.error("SchemaHistory Controller getPagedDatasets wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", SchemaHistoryDAO.getPagedSchemaDataset(name, datasetId, page, size));
        return ok(result);
    }

    public static Result getSchemaHistory(int id)
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("data", Json.toJson(SchemaHistoryDAO.getSchemaHistoryByDatasetID(id)));
        return ok(result);
    }
}