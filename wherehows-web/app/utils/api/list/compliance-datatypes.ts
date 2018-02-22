import { IComplianceDataType, IComplianceDataTypeResponse } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { getListUrlRoot } from 'wherehows-web/utils/api/list/shared';

/**
 * Defines the url endpoint for the list of dataset compliance data types and attributes
 * @type {string}
 */
const complianceDataTypesUrl = `${getListUrlRoot('v2')}/compliance-data-types`;

/**
 * Requests the list of compliance data types and the related attributes
 * @returns {Promise<Array<IComplianceDataType>>}
 */
const readComplianceDataTypes = async (): Promise<Array<IComplianceDataType>> => {
  const { complianceDataTypes = [] } = await getJSON<IComplianceDataTypeResponse>({
    url: complianceDataTypesUrl
  });

  return complianceDataTypes;
};

export { readComplianceDataTypes };
