import { IFeatureAspect } from '@datahub/metadata-types/types/entity/feature/feature-aspect';

/**
 * A metadata snapshot for a specific feature entity
 * @export
 * @interface IFeatureSnapshot
 */
export interface IFeatureSnapshot {
  // URN for the entity the metadata snapshot is associated with
  urn: string;
  // The list of metadata aspects associated with the feature.
  // Depending on the use case, this can either be all, or a selection, of supported aspects.
  aspects: Array<IFeatureAspect>;
}
