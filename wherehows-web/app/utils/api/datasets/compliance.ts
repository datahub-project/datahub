import Ember from 'ember';
import { createInitialComplianceInfo } from 'wherehows-web/utils/datasets/functions';
import { datasetUrlById } from 'wherehows-web/utils/api/datasets/shared';
import { ApiStatus } from 'wherehows-web/utils/api/shared';
import { IComplianceSuggestion, IComplianceSuggestionResponse } from 'wherehows-web/typings/api/datasets/compliance';

const { $: { getJSON }, assert } = Ember;

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
 * Fetches the current compliance policy for a dataset with thi given id
 * @param {number} id the id of the dataset
 * @return {Promise<{isNewComplianceInfo: boolean, complianceInfo: *}>}
 */
const datasetComplianceFor = async (id: number): Promise<{ isNewComplianceInfo: boolean; complianceInfo: any }> => {
  assert(`Expected id to be a number but received ${typeof id}`, typeof id === 'number');
  const failedStatus = 'failed';
  const notFound = 'actual 0';
  // complianceInfo contains the compliance data for the specified dataset
  let {
    msg = '',
    status,
    complianceInfo
  }: { msg: string; status: string; complianceInfo: any } = await Promise.resolve(
    getJSON(datasetComplianceUrlById(id))
  );
  // If the endpoint responds with a failed status, and the msg contains the indicator that a compliance does not exist
  const isNewComplianceInfo: boolean = status === failedStatus && String(msg).includes(notFound);

  if (isNewComplianceInfo) {
    complianceInfo = createInitialComplianceInfo(id);
  }

  return { isNewComplianceInfo, complianceInfo };
};

/**
 * Requests the compliance suggestions for a given dataset Id and returns the suggestion list
 * @param {number} id the id of the dataset
 * @return {Promise<Array<IComplianceSuggestion>>}
 */
const datasetComplianceSuggestionsFor = async (
  id: number
): Promise<{ complianceSuggestions: Array<IComplianceSuggestion>; lastModified: number | void }> => {
  const response: IComplianceSuggestionResponse = await Promise.resolve(
    getJSON(datasetComplianceSuggestionsUrlById(id))
  );
  const { status, autoClassification = { classificationResult: '[]', lastModified: 0 } } = response;
  let complianceSuggestions: Array<IComplianceSuggestion> = [];
  let lastModifiedDate;

  if (status === ApiStatus.OK) {
    const { classificationResult, lastModified } = autoClassification;

    try {
      complianceSuggestions = [...JSON.parse(classificationResult)];
      lastModifiedDate = lastModified;
    } catch (e) {
      throw e;
    }
  }

  return { complianceSuggestions, lastModified: lastModifiedDate };
};

export { datasetComplianceFor, datasetComplianceSuggestionsFor, datasetComplianceUrlById };
