import { IComplianceEntity, IComplianceInfo } from 'datahub-web/typings/api/datasets/compliance';
import { IComplianceChangeSet, ISchemaFieldsToPolicy } from 'datahub-web/typings/app/dataset-compliance';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';

/**
 * Defines the interface for keys extracted from the columns property on an response of IDatasetSchemaGetResponse
 * @interface IColumnFieldProps
 */
export interface IColumnFieldProps {
  identifierField: IDatasetSchemaColumn['fullFieldPath'];
  dataType: IDatasetSchemaColumn['dataType'];
  identifierType?: IComplianceEntity['identifierType'];
  logicalType?: IComplianceEntity['logicalType'];
  suggestion?: IComplianceChangeSet['suggestion'];
  suggestionAuthority?: IComplianceChangeSet['suggestionAuthority'];
}

/**
 * Defines the interface for properties passed into the mapping function asyncMapSchemaColumnPropsToCurrentPrivacyPolicy
 * @interface ISchemaColumnMappingProps
 */
export interface ISchemaColumnMappingProps {
  columnProps: Array<IColumnFieldProps>;
  complianceEntities: IComplianceInfo['complianceEntities'];
  policyModificationTime: IComplianceInfo['modifiedTime'];
}

/**
 * Describes the function interface for the mapping reducer function that takes current entities and modification time
 * and returns a function that accumulates an instance of ISchemaFieldsToPolicy
 * @interface ISchemaWithPolicyTagsReducingFn
 */
export interface ISchemaWithPolicyTagsReducingFn {
  (currentEntities: IComplianceInfo['complianceEntities'], policyModificationTime: IComplianceInfo['modifiedTime']): (
    schemaFieldsToPolicy: ISchemaFieldsToPolicy,
    props: IColumnFieldProps
  ) => ISchemaFieldsToPolicy;
}
