import { FrameFeatureCategory } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-category';
import { FeatureInferType } from '@datahub/metadata-types/constants/entity/feature/frame/feature-infer-type';

/**
 * Properties associated with a feature which can be modified/edited by the user
 * @export
 * @interface IFrameFeatureEditableConfig
 */
export interface IFrameFeatureEditableConfig {
  // Description of the feature or link to the documentation of the feature
  description?: string;
  // Category of the feature
  category?: {
    categoryString: FrameFeatureCategory;
  };
  // Link to the documentation of the feature
  documentationLink?: string;
  // Base category of the feature, e.g. Member, Job etc
  baseEntity?: {
    baseEntityString: string;
  };
  // Classification category of the feature, e.g. Characteristic, Activity etc
  classification?: {
    classificationString: string;
  };
  // Infer type of the feature, e.g. Fact or Inferred
  inferType?: FeatureInferType;
}
