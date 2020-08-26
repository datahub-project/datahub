import { FeatureTierType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-tier-type';
/**
 * Properties associated with the tier of a feature which can be modified/edited by the user
 * @export
 * @interface IFrameFeatureTierConfig
 */
export interface IFrameFeatureTierConfig {
  // Tier of the feature
  tier?: FeatureTierType;
}
