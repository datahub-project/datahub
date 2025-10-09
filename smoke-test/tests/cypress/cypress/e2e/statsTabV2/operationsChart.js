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

const formatDay = (day) => day.format("YYYY-MM-DD");

describe("Change history calendar chart", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  const patchOperationsData = () => {
    const today = dayjs().startOf("day");
    patchResponse("getOperationsStatsBuckets", (data) =>
      patchObject(data, {
        "dataset.operationsStats": {
          aggregations: {
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            customOperationsMap: [{ key: "custom_type", value: 1 }],
          },
          buckets: [
            {
              bucket: today.valueOf(),
              aggregations: {
                totalCreates: 1,
              },
            },
            {
              bucket: today.subtract(1, "day").valueOf(),
              aggregations: {
                totalUpdates: 1,
              },
            },
            {
              bucket: today.subtract(2, "days").valueOf(),
              aggregations: {
                totalDeletes: 1,
              },
            },
            {
              bucket: today.subtract(3, "days").valueOf(),
              aggregations: {
                totalInserts: 1,
              },
            },
            {
              bucket: today.subtract(4, "days").valueOf(),
              aggregations: {
                totalAlters: 1,
              },
            },
            {
              bucket: today.subtract(5, "days").valueOf(),
              aggregations: {
                totalDrops: 1,
              },
            },
            {
              bucket: today.subtract(7, "days").valueOf(),
              aggregations: {
                totalCustoms: 1,
                customOperationsMap: [{ key: "custom_type", value: 1 }],
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
          totalUpdates: 1,
          totalDeletes: 1,
          totalInserts: 1,
          totalAlters: 1,
          totalDrops: 1,
          totalCustoms: 1,
          totalOperations: 7,
          customOperationsMap: [{ key: "custom_type", value: 1 }],
        },
      }),
    );

    ApiResponseHelpers.patchGetDatasetTimeseriesCapability({
      "dataset.timeseriesCapabilities.assetStats.oldestOperationTime": today
        .subtract(7, "day")
        .valueOf(),
    });
  };

  it("should be available when there are some data", () => {
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

    StatsTabHelper.changeHistoryChart.ensureChartVisible();
  });

  it("should be empty when there are no any data", () => {
    openStatsTabOfSampleEntity();

    StatsTabHelper.changeHistoryChart.ensureChartHasNoData();
  });

  it("should not be available when user has no permissions", () => {
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
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [
          ApiResponseHelpers.getSampleProfile(Date.now()),
        ],
        "dataset.siblingsSearch.searchResults[0].entity.privileges.canViewDatasetOperations": false,
      }),
    );

    openStatsTabOfSampleEntity();

    StatsTabHelper.changeHistoryChart.ensureHasNoPermissions();
  });

  it("should show popover with correct information", () => {
    const today = dayjs().startOf("day");
    patchOperationsData();

    openStatsTabOfSampleEntity();

    // days with operations
    [
      {
        day: formatDay(today),
        totalText: "1 Change",
        changes: [
          {
            type: StatsTabHelper.changeHistoryChart.OPERATION_TYPE_CREATE,
            name: "Creates",
            value: "1",
          },
        ],
      },
      {
        day: formatDay(today.subtract(1, "day")),
        totalText: "1 Change",
        changes: [
          {
            type: StatsTabHelper.changeHistoryChart.OPERATION_TYPE_UPDATE,
            name: "Updates",
            value: "1",
          },
        ],
      },
      {
        day: formatDay(today.subtract(2, "day")),
        totalText: "1 Change",
        changes: [
          {
            type: StatsTabHelper.changeHistoryChart.OPERATION_TYPE_DELETE,
            name: "Deletes",
            value: "1",
          },
        ],
      },
      {
        day: formatDay(today.subtract(3, "day")),
        totalText: "1 Change",
        changes: [
          {
            type: StatsTabHelper.changeHistoryChart.OPERATION_TYPE_INSERT,
            name: "Inserts",
            value: "1",
          },
        ],
      },
      {
        day: formatDay(today.subtract(4, "day")),
        totalText: "1 Change",
        changes: [
          {
            type: StatsTabHelper.changeHistoryChart.OPERATION_TYPE_ALTER,
            name: "Alters",
            value: "1",
          },
        ],
      },
      {
        day: formatDay(today.subtract(5, "day")),
        totalText: "1 Change",
        changes: [
          {
            type: StatsTabHelper.changeHistoryChart.OPERATION_TYPE_DROP,
            name: "Drops",
            value: "1",
          },
        ],
      },
      {
        day: formatDay(today.subtract(5, "day")),
        totalText: "1 Change",
        changes: [
          {
            type: StatsTabHelper.changeHistoryChart.OPERATION_TYPE_DROP,
            name: "Drops",
            value: "1",
          },
        ],
      },
      {
        day: formatDay(today.subtract(7, "day")),
        totalText: "1 Change",
        changes: [
          {
            type: "custom_custom_type",
            name: "custom_type",
            value: "1",
          },
        ],
      },
    ].forEach((dayPopover) => {
      StatsTabHelper.changeHistoryChart.ensureDayPopoverHasValue(
        dayPopover.day,
        dayPopover.totalText,
        dayPopover.changes,
      );
    });

    // day without operations
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(6, "day")),
    );

    // day with no data reported
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoDataReported(
      formatDay(today.subtract(8, "day")),
    );
  });

  it("should allow to filter changes by types select", () => {
    const today = dayjs().startOf("day");
    patchOperationsData();

    openStatsTabOfSampleEntity();

    StatsTabHelper.changeHistoryChart.toggleTypesSelectOptions([
      StatsTabHelper.changeHistoryChart.OPERATION_TYPE_CREATE,
      StatsTabHelper.changeHistoryChart.OPERATION_TYPE_ALTER,
      StatsTabHelper.changeHistoryChart.OPERATION_TYPE_DELETE,
      StatsTabHelper.changeHistoryChart.OPERATION_TYPE_DROP,
      StatsTabHelper.changeHistoryChart.OPERATION_TYPE_INSERT,
      StatsTabHelper.changeHistoryChart.OPERATION_TYPE_UPDATE,
    ]);

    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(1, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(2, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(3, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(4, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(5, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(6, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasValue(
      formatDay(today.subtract(7, "days")),
      "1 Change",
      [
        {
          type: "custom_custom_type",
          name: "custom_type",
          value: "1",
        },
      ],
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoDataReported(
      formatDay(today.subtract(8, "days")),
    );
  });

  it("should allow to filter changes by pills", () => {
    const today = dayjs().startOf("day");
    patchOperationsData();

    openStatsTabOfSampleEntity();

    StatsTabHelper.changeHistoryChart.toggleSummaryPill("custom_custom_type");

    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(1, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(2, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(3, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(4, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(5, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoOperations(
      formatDay(today.subtract(6, "days")),
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasValue(
      formatDay(today.subtract(7, "days")),
      "1 Change",
      [
        {
          type: "custom_custom_type",
          name: "custom_type",
          value: "1",
        },
      ],
    );
    StatsTabHelper.changeHistoryChart.ensureDayPopoverHasNoDataReported(
      formatDay(today.subtract(8, "days")),
    );
  });

  it("should allow to open the day drawler", () => {
    const today = dayjs().startOf("day");
    patchOperationsData();

    openStatsTabOfSampleEntity();

    StatsTabHelper.changeHistoryChart.openDayDrawer(formatDay(today));
    StatsTabHelper.changeHistoryChart.ensureDayDrawerIsVisible();
    StatsTabHelper.changeHistoryChart.closeDayDrawer();
  });
});
