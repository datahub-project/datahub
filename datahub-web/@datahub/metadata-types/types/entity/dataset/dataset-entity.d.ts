import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

/**
 * TODO META-11674
 *
 * DEPRECATED This model correspond to
 * https://jarvis.corp.linkedin.com/codesearch/result/?name=DatasetView.java&path=wherehows-frontend%2Fdatahub-dao%2Fsrc%2Fmain%2Fjava%2Fcom%2Flinkedin%2Fdatahub%2Fmodels%2Fview&reponame=wherehows%2Fwherehows-frontend
 *
 * which should be avoided but still used in some places
 */
export interface IDatasetEntity {
  createdTime: number;
  decommissionTime: number | null;
  // Whether or not the dataset has been deprecated (slightly different from removed)
  deprecated: boolean | null;
  // Any note that was added when the dataset was deprecated
  deprecationNote: string | null;
  // Description of the dataset, if any
  description: string;
  // Fabric/environment the dataset exists in
  fabric: FabricType;
  // Timestamp for last modified time
  modifiedTime: number;
  // Human readable (ish) name for the dataset
  nativeName: string;
  nativeType: string;
  // Platform on which the dataset exists
  platform: DatasetPlatform;
  properties: string | null;
  // Whether the dataset has been removed
  removed: boolean;
  tags: Array<string>;
  // Equivalent to urn of a dataset
  uri: string;
  // The health score for the dataset entity
  healthScore: number;
  // Open source support only, this property accesses a variety of properties of the dataset that are to be presented
  customProperties?: Com.Linkedin.Dataset.DatasetProperties['customProperties'];
}
