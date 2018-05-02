import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';
import { IComplianceEntity, IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { IComplianceChangeSet, ISchemaFieldsToPolicy } from 'wherehows-web/typings/app/dataset-compliance';

/**
 * Defines the interface for keys extracted from the columns property on an response of IDatasetSchemaGetResponse
 * @interface IColumnFieldProps
 */
interface IColumnFieldProps {
  identifierField: IDatasetColumn['fullFieldPath'];
  dataType: IDatasetColumn['dataType'];
  identifierType?: IComplianceEntity['identifierType'];
  logicalType?: IComplianceEntity['logicalType'];
  suggestion?: IComplianceChangeSet['suggestion'];
  suggestionAuthority?: IComplianceChangeSet['suggestionAuthority'];
}

/**
 * Defines the interface for properties passed into the mapping function asyncMapSchemaColumnPropsToCurrentPrivacyPolicy
 * @interface ISchemaColumnMappingProps
 */
interface ISchemaColumnMappingProps {
  columnProps: Array<IColumnFieldProps>;
  complianceEntities: IComplianceInfo['complianceEntities'];
  policyModificationTime: IComplianceInfo['modifiedTime'];
}

/**
 * Describes the function interface for the mapping reducer function that takes current entities and modification time
 * and returns a function that accumulates an instance of ISchemaFieldsToPolicy
 * @interface ISchemaWithPolicyTagsReducingFn
 */
interface ISchemaWithPolicyTagsReducingFn {
  (currentEntities: IComplianceInfo['complianceEntities'], policyModificationTime: IComplianceInfo['modifiedTime']): (
    schemaFieldsToPolicy: ISchemaFieldsToPolicy,
    props: IColumnFieldProps
  ) => ISchemaFieldsToPolicy;
}

export { IColumnFieldProps, ISchemaColumnMappingProps, ISchemaWithPolicyTagsReducingFn };
