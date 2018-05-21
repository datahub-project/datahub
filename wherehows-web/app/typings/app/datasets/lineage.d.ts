import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';

/**
 * Defines the interface for the attributes used by the app to render the upstream datasets for a child
 * dataset
 * @interface IUpstreamWithComplianceMetadata
 */
interface IUpstreamWithComplianceMetadata {
  urn: IDatasetView['uri'];
  nativeName: IDatasetView['nativeName'];
  hasCompliance: boolean;
}

export { IUpstreamWithComplianceMetadata };
