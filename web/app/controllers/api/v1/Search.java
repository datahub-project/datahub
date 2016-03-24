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
import dao.SearchDAO;
import models.DatasetColumn;
import play.api.libs.json.JsValue;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import org.apache.commons.lang3.StringUtils;
import dao.DatasetsDAO;

import java.util.List;

public class Search extends Controller
{
    public static Result getSearchAutoComplete()
    {
        ObjectNode result = Json.newObject();
        result.put("status", "ok");
        result.set("source", Json.toJson(SearchDAO.getAutoCompleteList()));

        return ok(result);
    }

    public static Result searchByKeyword()
    {
        ObjectNode result = Json.newObject();

        int page = 1;
        int size = 10;
        String keyword = request().getQueryString("keyword");
        String category = request().getQueryString("category");
        String source = request().getQueryString("source");
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
                Logger.error("Dataset Controller searchByKeyword wrong page parameter. Error message: " +
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
                Logger.error("Dataset Controller searchByKeyword wrong page parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        Boolean isDefault = false;
        if (StringUtils.isBlank(category))
        {
            category = "datasets";
        }
        if (StringUtils.isBlank(source))
        {
            source = "all";
        }
        else if (source.equalsIgnoreCase("default"))
        {
            source = "all";
            isDefault = true;
        }
        if (category.toLowerCase().equalsIgnoreCase("metrics"))
        {
            result.set("result", SearchDAO.getPagedMetricByKeyword(category, keyword, page, size));
        }
        else if (category.toLowerCase().equalsIgnoreCase("flows"))
        {
            result.set("result", SearchDAO.getPagedFlowByKeyword(category, keyword, page, size));
        }
        else if (category.toLowerCase().equalsIgnoreCase("jobs"))
        {
            result.set("result", SearchDAO.getPagedJobByKeyword(category, keyword, page, size));
        }
        else if (category.toLowerCase().equalsIgnoreCase("comments"))
        {
            result.set("result", SearchDAO.getPagedCommentsByKeyword(category, keyword, page, size));
        }
        else
        {
            ObjectNode node = SearchDAO.getPagedDatasetByKeyword(category, keyword, source, page, size);
            if (isDefault && node != null && node.has("count"))
            {
                Long count = node.get("count").asLong();
                if (count != null && count == 0)
                {
                    node = SearchDAO.getPagedFlowByKeyword("flows", keyword, page, size);
                    if (node!= null && node.has("count"))
                    {
                        Long flowCount = node.get("count").asLong();
                        if (flowCount != null && flowCount == 0)
                        {
                            node = SearchDAO.getPagedJobByKeyword("jobs", keyword, page, size);
                        }
                    }
                }
            }
            result.set("result", node);
        }

        return ok(result);
    }


}