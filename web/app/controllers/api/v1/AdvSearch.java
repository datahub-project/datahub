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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dao.AdvSearchDAO;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

public class AdvSearch extends Controller
{
    public static Result getDatasetSources()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("sources", Json.toJson(AdvSearchDAO.getDatasetSources()));

        return ok(result);
    }

    public static Result getDatasetScopes()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("scopes", Json.toJson(AdvSearchDAO.getDatasetScopes()));

        return ok(result);
    }

    public static Result getDatasetTableNames()
    {
        ObjectNode result = Json.newObject();
        String scopes = request().getQueryString("scopes");
        result.put("status", "ok");
        result.set("tables", Json.toJson(AdvSearchDAO.getTableNames(scopes)));

        return ok(result);
    }

    public static Result getDatasetFields()
    {
        ObjectNode result = Json.newObject();
        String tables = request().getQueryString("tables");
        result.put("status", "ok");
        result.set("fields", Json.toJson(AdvSearchDAO.getFields(tables)));

        return ok(result);
    }

    public static Result search()
    {
        ObjectNode result = Json.newObject();
        String searchOptStr = request().getQueryString("searchOpts");
        JsonNode searchOpt = Json.parse(searchOptStr);
        int page = 1;
        int size = 10;
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
                Logger.error("AdvSearch Controller search wrong page parameter. Error message: " +
                        e.getMessage());
                page = 1;
            }
        }

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
                Logger.error("AdvSearch Controller search wrong page parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }
        result.put("status", "ok");
        result.set("result", Json.toJson(AdvSearchDAO.search(searchOpt, page, size)));

        return ok(result);
    }

}