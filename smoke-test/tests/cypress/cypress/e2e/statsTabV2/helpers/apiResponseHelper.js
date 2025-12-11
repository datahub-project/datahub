/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import { patchObject, patchResponse } from "../../utils";

export class ApiResponseHelpers {
  static getSampleProfile(timestamp) {
    return {
      rowCount: 1,
      columnCount: 1,
      sizeInBytes: 1024,
      timestampMillis: timestamp,
      partitionSpec: null,
      fieldProfiles: [
        {
          fieldPath: "customer_id",
          max: null,
          mean: null,
          median: null,
          min: null,
          nullCount: 0,
          nullProportion: 0,
          quantiles: null,
          sampleValues: ["1", "2"],
          distinctValueFrequencies: null,
          stdev: null,
          uniqueCount: 2,
          uniqueProportion: 1,
          __typename: "DatasetFieldProfile",
        },
      ],
      __typename: "DatasetProfile",
    };
  }

  static getSampleUsageStats(timestamp) {
    return {
      buckets: [
        {
          bucket: timestamp,
          metrics: {
            totalSqlQueries: 10,
            __typename: "UsageAggregationMetrics",
          },
          __typename: "UsageAggregation",
        },
      ],
      aggregations: {
        uniqueUserCount: 1,
        totalSqlQueries: 10,
        fields: [
          {
            fieldName: "testField",
            count: 10,
            __typename: "FieldUsageCounts",
          },
        ],
        __typename: "UsageQueryResultAggregations",
      },
      __typename: "UsageQueryResult",
    };
  }

  static patchGetDataProfiles(patch) {
    patchResponse("getDataProfiles", (data) => patchObject(data, patch));
  }

  static patchGetDatasetTimeseriesCapability(patch) {
    patchResponse("getDatasetTimeseriesCapability", (data) =>
      patchObject(data, patch),
    );
  }
}
