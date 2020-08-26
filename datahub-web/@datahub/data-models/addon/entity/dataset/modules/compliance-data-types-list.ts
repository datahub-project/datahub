import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';
import { readComplianceDataTypes } from '@datahub/data-models/api/dataset/compliance';
import { set } from '@ember/object';

/**
 * A simple wrapper around the reader for a compliance data types list that helps us provide the read from
 * the data layer to the consuming application
 */
export class ComplianceDataTypesList {
  /**
   * The read list for the compliance data types, which are used to determine what options are available
   * for annoations of a dataset's fields
   * @type {Array<IComplianceDataType>}
   */
  list?: Array<IComplianceDataType>;

  /**
   * Read function to fetch the compliance data types from the API layer
   */
  readDataTypesList(): Promise<Array<IComplianceDataType>> {
    return readComplianceDataTypes();
  }
}

/**
 * Custom factory for the data types list factory, useful as this does not follow the typical creator
 * for a base entity.
 */
export const createComplianceDataTypesList = async (): Promise<ComplianceDataTypesList> => {
  const dataTypesList = new ComplianceDataTypesList();
  const complianceDataTypes = await dataTypesList.readDataTypesList();

  set(dataTypesList, 'list', complianceDataTypes);
  return dataTypesList;
};
