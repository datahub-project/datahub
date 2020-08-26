import { IFrameFeatureEditableConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-editable-config';
import { IFrameFeatureConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-config';
import { IBaseAspect } from '@datahub/metadata-types/types/metadata/aspect';
import { IFrameFeatureAvailabilityConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-availability-config';
import { IFrameFeatureStatusConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-status-config';
import { IFrameFeatureTierConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-tier-config';

/**
 * Supported metadata aspects for a Feature
 * @export
 * @interface IFeatureAspect
 * @extends {IBaseAspect}
 */
export interface IFeatureAspect extends IBaseAspect {
  urn: string;
  'com.linkedin.common.Ownership'?: Com.Linkedin.Common.Ownership;
  'com.linkedin.feature.frame.FrameFeatureAvailabilityConfig'?: IFrameFeatureAvailabilityConfig;
  'com.linkedin.feature.frame.FrameFeatureConfig'?: IFrameFeatureConfig;
  'com.linkedin.feature.frame.FrameFeatureEditableConfig'?: IFrameFeatureEditableConfig;
  'com.linkedin.feature.frame.FrameFeatureStatusConfig'?: IFrameFeatureStatusConfig;
  'com.linkedin.feature.frame.FrameFeatureTierConfig'?: IFrameFeatureTierConfig;
}
