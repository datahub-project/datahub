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
import dao.FlowsDAO;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import org.apache.commons.lang3.StringUtils;

public class Flow extends Controller
{
    public static Result getPagedRootProjects()
    {
        ObjectNode result = Json.newObject();
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
                Logger.error("Flow Controller getPagedRootProjects wrong page parameter. Error message: " +
                        e.getMessage());
                page = 1;
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
                Logger.error("Flow Controller getPagedRootProjects wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", FlowsDAO.getPagedProjects(page, size));
        return ok(result);
    }

    public static Result getPagedProjects(String application)
    {
        ObjectNode result = Json.newObject();
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
                Logger.error("Flow Controller getPagedDatasets wrong page parameter. Error message: " +
                        e.getMessage());
                page = 1;
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
                Logger.error("Flow Controller getPagedDatasets wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", FlowsDAO.getPagedProjectsByApplication(application, page, size));
        return ok(result);
    }

    public static Result getPagedFlows(String application, String project)
    {
        ObjectNode result = Json.newObject();
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
                Logger.error("Flow Controller getPagedDatasets wrong page parameter. Error message: " +
                        e.getMessage());
                page = 1;
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
                Logger.error("Flow Controller getPagedDatasets wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", FlowsDAO.getPagedFlowsByProject(application, project, page, size));
        return ok(result);
    }

    public static Result getPagedJobs(String application, Long flowId)
    {
        ObjectNode result = Json.newObject();
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
                Logger.error("Flow Controller getPagedDatasets wrong page parameter. Error message: " +
                        e.getMessage());
                page = 1;
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
                Logger.error("Flow Controller getPagedDatasets wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", FlowsDAO.getPagedJobsByFlow(application, flowId, page, size));
        return ok(result);
    }

    public static Result getFlowListViewClusters()
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("nodes", Json.toJson(FlowsDAO.getFlowListViewClusters()));
        return ok(result);
    }

    public static Result getFlowListViewProjects(String application)
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("nodes", Json.toJson(FlowsDAO.getFlowListViewProjects(application)));
        return ok(result);
    }

    public static Result getFlowListViewFlows(String application, String project)
    {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("nodes", Json.toJson(FlowsDAO.getFlowListViewFlows(application, project)));
        return ok(result);
    }
}
