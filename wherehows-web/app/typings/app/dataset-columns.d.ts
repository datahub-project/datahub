import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';
import { IComplianceEntity, IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { ISchemaFieldsToPolicy } from 'wherehows-web/typings/app/dataset-compliance';

/**
 * Defines the interface for keys extracted from the columns property on an response of IDatasetSchemaGetResponse
 * @interface IColumnFieldProps
 */
interface IColumnFieldProps {
  identifierField: IDatasetColumn['fullFieldPath'];
  dataType: IDatasetColumn['dataType'];
  identifierType?: IComplianceEntity['identifierType'];
  logicalType?: IComplianceEntity['logicalType'];
}

/**
 * Defines the interface for properties passed into the mapping function mapSchemaColumnPropsToCurrentPrivacyPolicy
 * @interface ISchemaColumnMappingProps
 */
interface ISchemaColumnMappingProps {
  columnProps: Array<IColumnFieldProps>;
  complianceEntities: IComplianceInfo['complianceEntities'];
  policyModificationTime: IComplianceInfo['modifiedTime'];
}

/**
 * Describes the function interface for the mapping reducer function that takes current entities and modification time
 * and returns a function that accumulates and instance of ISchemaFieldsToPolicy
 * @interface ICompliancePolicyReducerFactory
 */
interface ICompliancePolicyReducerFactory {
  (currentEntities: IComplianceInfo['complianceEntities'], policyModificationTime: IComplianceInfo['modifiedTime']): (
    acc: ISchemaFieldsToPolicy,
    props: IColumnFieldProps
  ) => ISchemaFieldsToPolicy;
}

export { IColumnFieldProps, ISchemaColumnMappingProps, ICompliancePolicyReducerFactory };
