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

import dao.DashboardDAO;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.mvc.Controller;
import play.mvc.Result;


public class Dashboard extends Controller {

  public static Result getOwnershipDatasetsPercentage(String managerId) {
    String platform = request().getQueryString("platform");
    return ok(DashboardDAO.getDashboardStatByUserId(managerId, platform, DashboardDAO.Category.Ownership));
  }

  public static Result getConfidentialDatasetsPercentage(String managerId) {
    String platform = request().getQueryString("platform");
    return ok(DashboardDAO.getDashboardStatByUserId(managerId, platform, DashboardDAO.Category.SecuritySpec));
  }

  public static Result getDescriptionDatasetsPercentage(String managerId) {
    String platform = request().getQueryString("platform");
    return ok(DashboardDAO.getDashboardStatByUserId(managerId, platform, DashboardDAO.Category.Description));
  }

  public static Result getIdpcComplianceDatasetsPercentage(String managerId) {
    String platform = request().getQueryString("platform");
    return ok(DashboardDAO.getDashboardStatByUserId(managerId, platform, DashboardDAO.Category.PrivacyCompliance));
  }

  public static Result getPagedOwnershipDatasets(String managerId) {
    String platform = request().getQueryString("platform");
    int option = parseInt(request().getQueryString("option"), 1);
    int page = parseInt(request().getQueryString("page"), 1);
    int size = parseInt(request().getQueryString("size"), 10);

    return ok(DashboardDAO.getPagedOwnershipDatasetsByManagerId(managerId, platform, option, page, size));
  }

  public static Result getPagedConfidentialDatasets(String managerId) {
    String platform = request().getQueryString("platform");
    int page = parseInt(request().getQueryString("page"), 1);
    int size = parseInt(request().getQueryString("size"), 10);

    return ok(DashboardDAO.getPagedConfidentialDatasetsByManagerId(managerId, platform, page, size));
  }

  public static Result getPagedDescriptionDatasets(String managerId) {
    String platform = request().getQueryString("platform");
    int option = parseInt(request().getQueryString("option"), 1);
    int page = parseInt(request().getQueryString("page"), 1);
    int size = parseInt(request().getQueryString("size"), 10);

    return ok(DashboardDAO.getPagedDescriptionDatasetsByManagerId(managerId, platform, option, page, size));
  }

  public static Result getPagedComplianceDatasets(String managerId) {
    String platform = request().getQueryString("platform");
    String option = request().getQueryString("option");
    int page = parseInt(request().getQueryString("page"), 1);
    int size = parseInt(request().getQueryString("size"), 10);

    return ok(DashboardDAO.getPagedComplianceDatasetsByManagerId(managerId, platform, option, page, size));
  }

  public static Result getOwnershipBarData(String managerId) {
    return ok(DashboardDAO.getOwnershipBarChartData(managerId));
  }

  public static Result getDescriptionBarData(String managerId) {
    int option = parseInt(request().getQueryString("option"), 1);

    return ok(DashboardDAO.getDescriptionBarChartData(managerId, option));
  }

  private static int parseInt(String queryString, int defaultValue) {
    if (!StringUtils.isBlank(queryString)) {
      try {
        return Integer.parseInt(queryString);
      } catch (NumberFormatException e) {
        Logger.error(
            "Dashboard Controller query string wrong parameter: " + queryString + ". Error message: " + e.getMessage());
      }
    }
    return defaultValue;
  }
}
