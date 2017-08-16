import Ember from 'ember';
/// <reference path='wherehows-web/typings/untyped-js-module' />
import { createInitialComplianceInfo } from 'wherehows-web/utils/datasets/functions';
import { apiRoot } from 'wherehows-web/utils/api';

const { $: { getJSON }, assert } = Ember;

/**
 * Defines the endpoint for datasets
 * @type {string}
 */
const datasetsUrlRoot = `${apiRoot}/datasets`;

/**
 * Constructs a url to get a dataset with a given id
 * @param {number} id the id of the dataset
 * @return {string} the dataset url
 */
const datasetUrlById = (id: number): string => `${datasetsUrlRoot}/${id}`;

/**
 * Constructs the dataset compliance url
 * @param {number} id the id of the dataset
 * @return {string} the dataset compliance url
 */
const datasetComplianceUrlById = (id: number): string => `${datasetUrlById(id)}/compliance`;

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

export { datasetsUrlRoot, datasetComplianceFor };
