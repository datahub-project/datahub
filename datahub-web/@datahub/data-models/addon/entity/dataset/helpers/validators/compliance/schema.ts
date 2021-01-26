import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import DatasetSchema from '@datahub/data-models/entity/dataset/modules/schema';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';

/**
 * Given a list of compliance annotations and a schema to match against, removes annotations for
 * which there is no matching schema field
 * @param {Array<DatasetComplianceAnnotation>} annotations - list of annotations to filter
 * @param {DatasetSchema} schema - schema to filter against
 */
export const filterAnnotationsToDatasetSchema = (
  annotations: Array<DatasetComplianceAnnotation>,
  schema: DatasetSchema
): Array<DatasetComplianceAnnotation> => {
  const { schemaFields } = schema;

  if (!schemaFields) {
    return annotations;
  }

  const schemaFieldSet: Set<IDatasetSchemaColumn['fullFieldPath']> = new Set();
  schemaFields.forEach((field): void => {
    schemaFieldSet.add(field.fullFieldPath);
  });

  return annotations.filter((annotation): boolean => schemaFieldSet.has(annotation.identifierField));
};
