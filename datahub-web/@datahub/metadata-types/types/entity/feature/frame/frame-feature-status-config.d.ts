import { FeatureStatusType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-status-type';

/**
 * Properties associated with the status of a feature which can be modified/edited by the user
 * @export
 * @interface IFrameFeatureStatusConfig
 */
export interface IFrameFeatureStatusConfig {
  // Status of the feature
  // default FeatureStatusType.Unpublished
  status: FeatureStatusType;
}
