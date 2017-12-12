import { IComplianceDataType, IComplianceDataTypeResponse } from 'wherehows-web/typings/api/list/compliance-datatypes';
import { ApiStatus } from 'wherehows-web/utils/api';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { listUrlRoot } from 'wherehows-web/utils/api/list/shared';

/**
 * Defines the url endpoint for the list of dataset compliance data types and attributes
 * @type {string}
 */
const complianceDataTypesUrl = `${listUrlRoot}/complianceDataTypes`;

/**
 * Requests the list of compliance data types and the related attributes
 * @returns {Promise<Array<IComplianceDataType>>}
 */
const readComplianceDataTypes = async (): Promise<Array<IComplianceDataType>> => {
  const { status, complianceDataTypes = [], msg } = await getJSON<IComplianceDataTypeResponse>({
    url: complianceDataTypesUrl
  });

  if (status === ApiStatus.OK && complianceDataTypes.length) {
    return complianceDataTypes;
  }

  throw new Error(msg);
};

export { readComplianceDataTypes };
