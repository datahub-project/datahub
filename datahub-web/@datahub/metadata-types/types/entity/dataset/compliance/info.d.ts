import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';
import { Classification } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';
import { DatasetClassification } from '@datahub/metadata-types/constants/entity/dataset/compliance/classifiers';

export interface IDatasetComplianceInfo {
  // List of compliance annotation/tag attributes for fields on the dataset
  complianceEntities: Array<IComplianceFieldAnnotation>;
  // User entered purge notation for a dataset with a purge exempt policy
  compliancePurgeNote: null | string;
  // Purge Policy for the dataset
  complianceType: PurgePolicy | '';
  // Dataset level security classification
  confidentiality: Classification | null;
  // Flag indicating that the dataset contains pii data, typically for schemaless dataset this is user entered,
  // for datasets with a schema, this derived from the complianceEntities
  containingPersonalData?: boolean | null;
  // Tags for a types of data contained in the related dataset
  datasetClassification: DatasetClassification | null;
  // Unique wherehows specific database identifier, at this point is legacy
  datasetId: null;
  // Unique urn for the dataset
  readonly datasetUrn?: string;
  // optional string with username of modifier
  modifiedBy?: string;
  // optional timestamp of last modification date
  modifiedTime?: number;
  // optional attribute indicating that the compliance policy is derived from a parent in the lineage
  readonly fromUpstream?: boolean;
}
