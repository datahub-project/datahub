import { IAvailabilityInfo } from '@datahub/metadata-types/types/entity/feature/frame/availability-info';

/**
 * Properties associated with the availability of a feature
 * @export
 * @interface IFrameFeatureAvailabilityConfig
 */
export interface IFrameFeatureAvailabilityConfig {
  // Availability of the feature in different environments like offline, online and nearline and fabrics like ei, prod etc
  availability: Array<IAvailabilityInfo>;
}
