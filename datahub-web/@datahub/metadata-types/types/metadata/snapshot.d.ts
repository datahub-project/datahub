import { IDatasetSnapshot } from '@datahub/metadata-types/types/metadata/dataset-snapshot';
import { IFeatureSnapshot } from '@datahub/metadata-types/types/metadata/feature-snapshot';

/**
 * Intersection of available entity snapshot types
 * @export
 * @namespace metadata.snapshot
 * @interface {IDatasetSnapshot & IFeatureSnapshot}
 */
export type Snapshot = IDatasetSnapshot & IFeatureSnapshot;
