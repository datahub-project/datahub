import { FeatureAggregationType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-aggregation-type';

/**
 * Properties associated with Sliding Window Aggregation feature
 * @export
 * @interface IFrameSlidingWindowAggregationInfo
 */
export interface IFrameSlidingWindowAggregationInfo {
  // Aggregation function to be applied for sliding window aggregation
  aggregation: FeatureAggregationType;
  // Length of window time, supports 4 type of units: d(day), h(hour), m(minute), s(second). The example value are 7d, 5h, 3m or 1s
  window: string;
  // A Spark SQL expression for filtering the fact data before aggregation
  filter?: string;
  // The column/field on which the data will be grouped by before aggregation
  groupBy?: string;
  // A number specifying for each group, taking the records with the TOP k aggregation value
  limit?: number;
}
