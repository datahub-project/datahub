/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import { patchObject, patchResponse } from "../utils";
import { ApiResponseHelpers } from "./helpers/apiResponseHelper";
import { StatsTabHelper } from "./helpers/statsTabHelper";

const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)";
const DATASET_NAME = "customers";

const openSampleEntity = () => {
  cy.goToDataset(DATASET_URN, DATASET_NAME);
};

describe("Stats tab on entity", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("should be disabled when entity has no latestFullTableProfile, latestPartitionProfile or usageStats", () => {
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [],
        "dataset.latestPartitionProfile": [],
        "dataset.usageStats.buckets": [],
      }),
    );

    openSampleEntity();

    StatsTabHelper.ensureTabIsDisabled();
  });

  it("should be enabled when entity has latestFullTableProfile", () => {
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [
          ApiResponseHelpers.getSampleProfile(Date.now()),
        ],
        "dataset.latestPartitionProfile": [],
        "dataset.usageStats.buckets": [],
      }),
    );

    openSampleEntity();

    StatsTabHelper.ensureTabIsEnabled();
  });

  it("should be enabled when entity has latestPartitionProfile", () => {
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [],
        "dataset.latestPartitionProfile": [
          ApiResponseHelpers.getSampleProfile(Date.now()),
        ],
        "dataset.usageStats.buckets": [],
      }),
    );

    openSampleEntity();

    StatsTabHelper.ensureTabIsEnabled();
  });

  it("should be enabled when entity has usageStats", () => {
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [],
        "dataset.latestPartitionProfile": [],
        "dataset.usageStats": ApiResponseHelpers.getSampleUsageStats(
          Date.now(),
        ),
      }),
    );

    openSampleEntity();

    StatsTabHelper.ensureTabIsEnabled();
  });

  it("should be clickable", () => {
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [
          ApiResponseHelpers.getSampleProfile(Date.now()),
        ],
      }),
    );

    openSampleEntity();
    StatsTabHelper.clickOnTab();

    StatsTabHelper.ensureTabIsSelected();
  });
});
