import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { AvailabilityEnvironmentType } from '@datahub/metadata-types/constants/entity/feature/frame/availability-environment-type';

/**
 * Availability of the feature or anchor in different environments
 * @export
 * @interface IAvailabilityInfo
 */
export interface IAvailabilityInfo {
  // Represents the environment where the feature/anchor is available, e.g. OFFLINE, ONLINE and NEARLINE
  environment: AvailabilityEnvironmentType;
  // Fabric where feature/anchor is available in combination with the environment
  fabric: FabricType;
}
