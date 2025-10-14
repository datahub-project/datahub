import dayjs from "dayjs";
import { patchObject, patchResponse } from "../utils";
import { ApiResponseHelpers } from "./helpers/apiResponseHelper";
import { StatsTabHelper } from "./helpers/statsTabHelper";

const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)";
const DATASET_NAME = "customers";

const openStatsTabOfSampleEntity = () => {
  StatsTabHelper.goToTab(DATASET_URN, DATASET_NAME);
};

describe("Highlights", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  const patchOldestDatasetProfileTime = (timestamp) => {
    ApiResponseHelpers.patchGetDatasetTimeseriesCapability({
      "dataset.timeseriesCapabilities.assetStats.oldestDatasetProfileTime":
        timestamp,
    });
  };

  it("should show values when the data is available", () => {
    const timestamp = dayjs().startOf("day").toDate().getTime();

    patchOldestDatasetProfileTime(timestamp);
    patchResponse("getDataProfiles", (data) =>
      patchObject(data, {
        "dataset.datasetProfiles": [
          ApiResponseHelpers.getSampleProfile(Date.now()),
        ],
      }),
    );
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [
          ApiResponseHelpers.getSampleProfile(Date.now()),
        ],
        "dataset.latestPartitionProfile": [],
      }),
    );
    patchResponse("getLastMonthUsageAggregations", (data) =>
      patchObject(data, {
        "dataset.usageStats.aggregations.totalSqlQueries": 1,
        "dataset.usageStats.aggregations.uniqueUserCount": 1,
        "dataset.usageStats.aggregations.users": [
          {
            count: 1,
            user: {
              __typename: "CorpUser",
              urn: "urn:li:corpuser:datahub",
              username: "datahub",
              type: "CORP_USER",
            },
          },
        ],
      }),
    );
    patchResponse("getTimeRangeUsageAggregations", (data) =>
      patchObject(data, {
        "dataset.usageStats.buckets": [
          {
            bucket: Date.now(),
            metrics: {
              totalSqlQueries: 1,
            },
            __typename: "UsageAggregation",
          },
        ],
      }),
    );
    patchResponse("getOperationsStatsBuckets", (data) =>
      patchObject(data, {
        "dataset.operationsStats": {
          aggregations: {
            totalCreates: 1,
          },
          buckets: [
            {
              bucket: Date.now(),
              aggregations: {
                totalCreates: 1,
              },
            },
          ],
        },
      }),
    );
    patchResponse("getOperationsStats", (data) =>
      patchObject(data, {
        "dataset.operationsStats.aggregations": {
          totalCreates: 1,
          totalOperations: 1,
        },
      }),
    );

    openStatsTabOfSampleEntity();

    StatsTabHelper.rowsCard.ensureHasValue("1");
    StatsTabHelper.rowsCard.ensureHasViewButton();
    StatsTabHelper.columnsCard.ensureHasValue("1");
    StatsTabHelper.columnsCard.ensureHasViewButton();
    StatsTabHelper.usersCard.ensureHasValue("1");
    StatsTabHelper.usersCard.ensureHasViewButton();
    StatsTabHelper.queriesCard.ensureHasValue("1");
    StatsTabHelper.queriesCard.ensureHasViewButton();
    StatsTabHelper.changesCard.ensureHasValue("1");
    StatsTabHelper.changesCard.ensureHasViewButton();
  });

  it("should not show values when the data is not available", () => {
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [
          patchObject(ApiResponseHelpers.getSampleProfile(Date.now()), {
            rowCount: null,
            columnCount: null,
            fieldProfiles: [],
          }),
        ],
        "dataset.latestPartitionProfile": [],
        "dataset.usageStats.buckets": [],
      }),
    );
    patchResponse("getDatasetSchema", (data) =>
      patchObject(data, {
        "dataset.schemaMetadata.fields": [],
        "dataset.siblingsSearch.searchResults[0].entity.schemaMetadata.fields":
          [],
      }),
    );

    openStatsTabOfSampleEntity();

    StatsTabHelper.rowsCard.ensureHasNoValue();
    StatsTabHelper.columnsCard.ensureHasNoValue();
    StatsTabHelper.usersCard.ensureHasValue("0");
    StatsTabHelper.queriesCard.ensureHasNoValue();
    StatsTabHelper.changesCard.ensureHasNoValue();
  });
});
