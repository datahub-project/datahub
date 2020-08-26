import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

/**
 * Defines the interface for the attributes used by the app to render the upstream datasets for a child
 * dataset
 * @interface IUpstreamWithComplianceMetadata
 */
export interface IUpstreamWithComplianceMetadata {
  urn: DatasetEntity['urn'];
  nativeName: DatasetEntity['name'];
  hasCompliance: boolean;
}
