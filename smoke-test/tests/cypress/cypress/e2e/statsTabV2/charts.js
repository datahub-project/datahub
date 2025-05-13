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

describe("Charts", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  const patchDataProfiles = (profiles) => {
    ApiResponseHelpers.patchGetDataProfiles({
      "dataset.datasetProfiles": profiles,
    });
  };

  const patchUsageBuckets = (buckets) => {
    patchResponse("getTimeRangeUsageAggregations", (data) =>
      patchObject(data, {
        "dataset.usageStats.buckets": buckets,
      }),
    );
  };

  const patchTimeseriesCapabilities = (timestamp) => {
    ApiResponseHelpers.patchGetDatasetTimeseriesCapability({
      "dataset.timeseriesCapabilities.assetStats.oldestDatasetProfileTime":
        timestamp,
      "dataset.timeseriesCapabilities.assetStats.oldestDatasetUsageTime":
        timestamp,
    });
  };

  const patchStatsResponse = (timestamp) => {
    if (timestamp) {
      patchTimeseriesCapabilities(timestamp);
      patchDataProfiles([ApiResponseHelpers.getSampleProfile(timestamp)]);
      patchUsageBuckets(
        ApiResponseHelpers.getSampleUsageStats(timestamp).buckets,
      );
    } else {
      patchTimeseriesCapabilities(null);
      patchDataProfiles([]);
      patchUsageBuckets([]);
    }
  };

  const forEachChart = (callback) => {
    callback(StatsTabHelper.rowsCoutChart);
    callback(StatsTabHelper.storageSizeChart);
    callback(StatsTabHelper.queryCountChart);
  };

  it("should be available when there are some data", () => {
    const timestamp = dayjs().startOf("day").valueOf();
    patchStatsResponse(timestamp);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => chart.ensureChartVisible());
  });

  it("should be empty when there are no any data", () => {
    patchStatsResponse(null);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => chart.ensureChartHasNoData());
  });

  it("should hide time filter when there are no data", () => {
    patchStatsResponse(null);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => chart.ensureTimeRangeSelectDoesNotExist());
  });

  it("should hide time filter when there is only one option", () => {
    const timestamp = dayjs().startOf("day").toDate().getTime();
    patchStatsResponse(timestamp);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => chart.ensureTimeRangeSelectDoesNotExist());
  });

  it("should show time filter with all options when an year of data available", () => {
    const timestamp = dayjs()
      .startOf("day")
      .subtract(1, "year")
      .toDate()
      .getTime();
    patchStatsResponse(timestamp);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => {
      chart.ensureChartVisible();
      chart.ensureTineRangeSelectHasSelectedValue(chart.TIME_RANGE_MONTH);
      chart.ensureTimeRangeSelectHasOptions([
        chart.TIME_RANGE_WEEK,
        chart.TIME_RANGE_MONTH,
        chart.TIME_RANGE_QUARTER,
        chart.TIME_RANGE_HALF_OF_YEAR,
        chart.TIME_RANGE_YEAR,
      ]);
    });
  });

  it("should show time filter with expected options when more then half of year of data available", () => {
    const timestamp = dayjs()
      .startOf("day")
      .subtract(6, "months")
      .subtract(1, "week")
      .toDate()
      .getTime();
    patchStatsResponse(timestamp);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => {
      chart.ensureChartVisible();
      chart.ensureTineRangeSelectHasSelectedValue(chart.TIME_RANGE_MONTH);
      chart.ensureTimeRangeSelectHasOptions([
        chart.TIME_RANGE_WEEK,
        chart.TIME_RANGE_MONTH,
        chart.TIME_RANGE_QUARTER,
        chart.TIME_RANGE_HALF_OF_YEAR,
        chart.TIME_RANGE_YEAR,
      ]);
    });
  });

  it("should show time filter with expected options when more then 3 months of data available", () => {
    const timestamp = dayjs()
      .startOf("day")
      .subtract(3, "months")
      .subtract(1, "week")
      .toDate()
      .getTime();
    patchStatsResponse(timestamp);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => {
      chart.ensureChartVisible();
      chart.ensureTineRangeSelectHasSelectedValue(chart.TIME_RANGE_MONTH);
      chart.ensureTimeRangeSelectHasOptions([
        chart.TIME_RANGE_WEEK,
        chart.TIME_RANGE_MONTH,
        chart.TIME_RANGE_QUARTER,
        chart.TIME_RANGE_HALF_OF_YEAR,
      ]);
      chart.ensureTimeRangeSelectHasNotOptions([chart.TIME_RANGE_YEAR]);
    });
  });

  it("should show time filter with expected options when more then month of data available", () => {
    const timestamp = dayjs()
      .startOf("day")
      .subtract(1, "month")
      .subtract(1, "week")
      .toDate()
      .getTime();
    patchStatsResponse(timestamp);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => {
      chart.ensureChartVisible();
      chart.ensureTineRangeSelectHasSelectedValue(chart.TIME_RANGE_MONTH);
      chart.ensureTimeRangeSelectHasOptions([
        chart.TIME_RANGE_WEEK,
        chart.TIME_RANGE_MONTH,
        chart.TIME_RANGE_QUARTER,
      ]);
      chart.ensureTimeRangeSelectHasNotOptions([
        chart.TIME_RANGE_HALF_OF_YEAR,
        chart.TIME_RANGE_YEAR,
      ]);
    });
  });

  it("should show time filter with expected options when more then week of data available", () => {
    const timestamp = dayjs()
      .startOf("day")
      .subtract(1, "week")
      .subtract(1, "day")
      .toDate()
      .getTime();
    patchStatsResponse(timestamp);

    openStatsTabOfSampleEntity();

    forEachChart((chart) => {
      chart.ensureChartVisible();
      chart.ensureTineRangeSelectHasSelectedValue(chart.TIME_RANGE_MONTH);
      chart.ensureTimeRangeSelectHasOptions([
        chart.TIME_RANGE_WEEK,
        chart.TIME_RANGE_MONTH,
      ]);
      chart.ensureTimeRangeSelectHasNotOptions([
        chart.TIME_RANGE_QUARTER,
        chart.TIME_RANGE_HALF_OF_YEAR,
        chart.TIME_RANGE_YEAR,
      ]);
    });
  });

  it("should not be available when user has no permissions", () => {
    patchResponse("getDataset", (data) =>
      patchObject(data, {
        "dataset.latestFullTableProfile": [
          ApiResponseHelpers.getSampleProfile(Date.now()),
        ],
        "dataset.siblingsSearch.searchResults[0].entity.privileges.canViewDatasetProfile": false,
        "dataset.siblingsSearch.searchResults[0].entity.privileges.canViewDatasetUsage": false,
      }),
    );

    openStatsTabOfSampleEntity();

    forEachChart((chart) => chart.ensureHasNoPermissions());
  });
});
