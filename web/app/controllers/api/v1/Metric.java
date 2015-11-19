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
import dao.MetricsDAO;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import org.apache.commons.lang3.StringUtils;
import java.util.Map;

public class Metric extends Controller
{
    public static Result getPagedMetrics()
    {

        ObjectNode result = Json.newObject();
        String username = session("user");

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
                Logger.warn("Metric Controller getPagedMetrics wrong page parameter. Error message: " +
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
                Logger.warn("Metric Controller getPagedMetrics wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", MetricsDAO.getPagedMetrics("", "", page, size, username));
        return ok(result);
    }

    public static Result getPagedMetricsByDashboard(String dashboardName)
    {

        ObjectNode result = Json.newObject();
        String username = session("user");

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
                Logger.warn("Metric Controller getPagedMetricsByDashboard wrong page parameter. Error message: " +
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
                Logger.warn("Metric Controller getPagedMetricsByDashboard wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", MetricsDAO.getPagedMetrics(dashboardName, "", page, size, username));
        return ok(result);
    }

    public static Result getPagedMetricsByDashboardandGroup(String dashboardName, String group)
    {

        ObjectNode result = Json.newObject();
        String username = session("user");

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
                Logger.warn("Metric Controller getPagedMetricsByDashboardandGroup wrong page parameter. Error message: " +
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
                Logger.warn("Metric Controller getPagedMetricsByDashboardandGroup wrong size parameter. Error message: " +
                        e.getMessage());
                size = 10;
            }
        }

        result.put("status", "ok");
        result.set("data", MetricsDAO.getPagedMetrics(dashboardName, group, page, size, username));
        return ok(result);
    }

    public static Result getMetricByID(int id)
    {
        String user = session("user");
        models.Metric metric = MetricsDAO.getMetricByID(id, user);

        ObjectNode result = Json.newObject();

        if (metric != null)
        {
            result.put("status", "ok");
            result.set("metric", Json.toJson(metric));
        }
        else
        {
            result.put("status", "error");
            result.put("message", "record not found");
        }

        return ok(result);
    }

    public static Result watchMetric(int id)
    {
        ObjectNode result = Json.newObject();
        String username = session("user");
        Map<String, String[]> params = request().body().asFormUrlEncoded();
        if (StringUtils.isNotBlank(username))
        {
            String message = MetricsDAO.watchMetric(id, params, username);
            if (StringUtils.isBlank(message))
            {
                result.put("status", "success");
            }
            else
            {
                result.put("status", "failed");
                result.put("message", message);
            }
        }
        else
        {
            result.put("status", "failed");
            result.put("message", "User is not authenticated");
        }

        return ok(result);
    }

    public static Result unwatchMetric(int id, int watchId)
    {
        ObjectNode result = Json.newObject();
        if (MetricsDAO.unwatch(watchId))
        {
            result.put("status", "success");
        }
        else
        {
            result.put("status", "failed");
        }

        return ok(result);
    }

    public static Result updateMetric(int id)
    {
        ObjectNode result = Json.newObject();
        Map<String, String[]> params = request().body().asFormUrlEncoded();

        String message = MetricsDAO.updateMetricValues(id, params);
        if (StringUtils.isBlank(message))
        {
            result.put("status", "success");
            return ok(result);
        }
        else
        {
            result.put("status", "failed");
            result.put("message", message);
            return badRequest(result);
        }

    }
}
