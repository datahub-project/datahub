import { IFeatureMultiProductInfo } from '@datahub/metadata-types/types/entity/feature/frame/feature-multiproduct-info';
import { FrameFeatureType } from '@datahub/metadata-types/constants/entity/feature/frame/frame-feature-type';
import { IFrameAnchorConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-anchor-config';
import { IFrameSlidingWindowAggregationInfo } from '@datahub/metadata-types/types/entity/feature/frame/frame-sliding-window-aggregation-info';
import { IFrameGlobalProperties } from '@datahub/metadata-types/types/entity/feature/frame/frame-global-properties';

/**
 * Feature properties as defined in the frame config.
 * @export
 * @interface IFrameFeatureConfig
 */
export interface IFrameFeatureConfig {
  // Name of the feature
  name: string;
  // Feature expression
  expression?: string;
  // Type of the feature
  type?: FrameFeatureType;
  // MVEL function or UDF defined in feature expression
  udf?: string;
  // Anchors of the feature as defined in all feature configs
  anchors: Array<IFrameAnchorConfig>;
  // Default value of a feature
  defaultValue?: string | number | Array<string> | Record<string, string>;
  // Properties of the multiproduct in which this feature is defined
  multiproductInfo?: IFeatureMultiProductInfo;
  // Properties associated with Sliding Window Aggregation feature
  slidingWindowAggregationInfo?: IFrameSlidingWindowAggregationInfo;
  // Global properties as defined in the globals section of frame config
  globalProperties: IFrameGlobalProperties;
  // Namespace of the feature, e.g. jymbii, waterloo, careers etc
  namespace: string;
}
