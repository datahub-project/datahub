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
