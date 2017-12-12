import { assert } from '@ember/debug';
import { createInitialComplianceInfo } from 'wherehows-web/utils/datasets/compliance-policy';
import { datasetUrlById } from 'wherehows-web/utils/api/datasets/shared';
import { ApiStatus } from 'wherehows-web/utils/api/shared';
import {
  IComplianceGetResponse,
  IComplianceInfo,
  IComplianceSuggestion,
  IComplianceSuggestionResponse
} from 'wherehows-web/typings/api/datasets/compliance';
import { getJSON } from 'wherehows-web/utils/api/fetcher';

/**
 * Constructs the dataset compliance url
 * @param {number} id the id of the dataset
 * @return {string} the dataset compliance url
 */
const datasetComplianceUrlById = (id: number): string => `${datasetUrlById(id)}/compliance`;

/**
 * Constructs the compliance suggestions url based of the compliance id
 * @param {number} id the id of the dataset
 * @return {string} compliance suggestions url
 */
const datasetComplianceSuggestionsUrlById = (id: number): string => `${datasetComplianceUrlById(id)}/suggestions`;

/**
 * Determines if the client app should 'new' a compliance policy
 * If the endpoint responds with a failed status, and the msg contains the indicator that a compliance does not exist
 * @param {IComplianceGetResponse} { status, msg }
 * @returns {boolean}
 */
const requiresCompliancePolicyCreation = ({ status, msg }: IComplianceGetResponse): boolean => {
  const notFound = 'No entity found for query';
  return status === ApiStatus.FAILED && String(msg).includes(notFound);
};

/**
 * Fetches the current compliance policy for a dataset with thi given id
 * @param {number} id the id of the dataset
 * @returns {(Promise<{ isNewComplianceInfo: boolean; complianceInfo: IComplianceInfo }>)}
 */
const readDatasetCompliance = async (
  id: number
): Promise<{ isNewComplianceInfo: boolean; complianceInfo: IComplianceInfo }> => {
  assert(`Expected id to be a number but received ${typeof id}`, typeof id === 'number');

  const response = await getJSON<IComplianceGetResponse>({ url: datasetComplianceUrlById(id) });
  const isNewComplianceInfo = requiresCompliancePolicyCreation(response);
  let { msg, status, complianceInfo } = response;

  if (isNewComplianceInfo) {
    complianceInfo = createInitialComplianceInfo(id);
  }

  if ((status === ApiStatus.OK || isNewComplianceInfo) && complianceInfo) {
    return { isNewComplianceInfo, complianceInfo };
  }

  throw new Error(msg);
};

/**
 * Requests the compliance suggestions for a given dataset Id and returns the suggestion list
 * @param {number} id the id of the dataset
 * @return {Promise<IComplianceSuggestion>}
 */
const readDatasetComplianceSuggestion = async (id: number): Promise<IComplianceSuggestion> => {
  const { complianceSuggestion = <IComplianceSuggestion>{} } = await getJSON<IComplianceSuggestionResponse>({
    url: datasetComplianceSuggestionsUrlById(id)
  });

  return complianceSuggestion;
};

export { readDatasetCompliance, readDatasetComplianceSuggestion, datasetComplianceUrlById };
