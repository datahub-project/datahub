import { IMirageDB } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';
import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';
import { IDatasetComplianceInfo } from '@datahub/metadata-types/types/entity/dataset/compliance/info';
import { IDatasetExportPolicy } from '@datahub/metadata-types/types/entity/dataset/compliance/export-policy';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';
import { IDatasetRetentionPolicy } from '@datahub/metadata-types/types/entity/dataset/compliance/retention';

type SchemaDb<T> = Array<T> & {
  where: (query: Partial<T>) => Array<T>;
  update: (query: Partial<T>) => void;
  remove: (query: Partial<T>) => void;
  insert: (item: T) => void;
};

/**
 * Used a backwards approach to develop this interface, which represents the data available in the route
 * handler function for our mirage configurations. Examined the object and saw that this was the format of
 * the available interfaces, and mapping them here specifically for this addon.
 */
export interface IMirageDatasetCoreSchema {
  complianceDataTypes: IMirageDB<IComplianceDataType>;
  db: {
    datasetComplianceAnnotationTags: SchemaDb<IComplianceFieldAnnotation & { isSuggestion: boolean }>;
    datasetSchemaColumns: SchemaDb<IDatasetSchemaColumn>;
    datasets: SchemaDb<Com.Linkedin.Dataset.Dataset>;
    datasetComplianceInfos: SchemaDb<IDatasetComplianceInfo>;
    datasetExportPolicies: SchemaDb<IDatasetExportPolicy>;
    datasetPurgePolicies: SchemaDb<IDatasetRetentionPolicy>;
    platforms: SchemaDb<IDataPlatform>;
  };
}
