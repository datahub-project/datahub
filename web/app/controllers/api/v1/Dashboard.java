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
import dao.DashboardDAO;
import dao.JiraDAO;
import dao.UserDAO;
import models.JiraTicket;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Dashboard extends Controller
{

    public static Result getConfidentialDatasetsPercentage(String managerId)
    {
        return ok(DashboardDAO.getConfidentialPercentageByManagerId(managerId));
    }

    public static Result getDescriptionDatasetsPercentage(String managerId)
    {
        return ok(DashboardDAO.getDescriptionPercentageByManagerId(managerId));
    }

    public static Result getPagedConfidentialDatasets(String managerId)
    {
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
                Logger.error("Dashboard Controller getPagedConfidentialDatasets wrong page parameter. Error message: " + e.getMessage());
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
                Logger.error("Dashboard Controller getPagedConfidentialDatasets wrong size parameter. Error message: " + e.getMessage());
                size = 10;
            }
        }

        return ok(DashboardDAO.getPagedConfidentialDatasetsByManagerId(managerId, page, size));
    }

    public static Result getPagedDescriptionDatasets(String managerId)
    {
        int page = 1;
        int option = 1;
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
                Logger.error("Dashboard Controller getPagedDescriptionDatasets wrong page parameter. Error message: " + e.getMessage());
                page = 1;
            }
        }

        String optionStr = request().getQueryString("option");
        if (StringUtils.isBlank(optionStr))
        {
            option = 1;
        }
        else
        {
            try
            {
                option = Integer.parseInt(optionStr);
            }
            catch(NumberFormatException e)
            {
                Logger.error("Dashboard Controller getPagedDescriptionDatasets wrong option parameter. Error message: " + e.getMessage());
                option = 1;
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
                Logger.error("Dashboard Controller getPagedDescriptionDatasets wrong size parameter. Error message: " + e.getMessage());
                size = 10;
            }
        }

        return ok(DashboardDAO.getPagedDescriptionDatasetsByManagerId(managerId, option, page, size));
    }

    public static Result getDescriptionBarData(String managerId)
    {
        int option = 1;

        String optionStr = request().getQueryString("option");
        if (StringUtils.isBlank(optionStr))
        {
            option = 1;
        }
        else
        {
            try
            {
                option = Integer.parseInt(optionStr);
            }
            catch(NumberFormatException e)
            {
                Logger.error("Dashboard Controller getPagedDescriptionDatasets wrong option parameter. Error message: " + e.getMessage());
                option = 1;
            }
        }

        return ok(DashboardDAO.getDescriptionBarChartData(managerId, option));
    }
}
