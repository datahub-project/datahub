import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';
import { IDatasetComplianceInfo } from '@datahub/metadata-types/types/entity/dataset/compliance/info';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { getListUrlRoot } from '@datahub/data-models/api/dataset/shared/lists';
import { ApiVersion } from '@datahub/utils/api/shared';
import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';
import { IDatasetRetentionPolicy } from '@datahub/metadata-types/types/entity/dataset/compliance/retention';
import { IDatasetExportPolicy } from '@datahub/metadata-types/types/entity/dataset/compliance/export-policy';
import { Omit } from 'lodash';
import { IDatasetComplianceSuggestionInfo } from '@datahub/data-models/types/entity/dataset';
import { SuggestionIntent } from '@datahub/data-models/constants/entity/dataset/compliance-suggestions';

/**
 * Returns the url for a datasets compliance policy by urn
 * @param {string} urn
 * @return {string}
 */
export const datasetComplianceUrl = (urn: string): string => `${datasetUrlByUrn(urn)}/compliance`;

/**
 * Returns the url for a dataset compliance suggestion by urn
 * @param {string} urn
 * @return {string}
 */
export const datasetComplianceSuggestionUrlByUrn = (urn: string): string =>
  `${datasetUrlByUrn(urn)}/compliance/suggestion`;

/**
 * Returns the url for a dataset compliance suggestion feedback by urn
 * @param {string} urn the urn for the dataset
 * @return {string}
 */
export const datasetComplianceSuggestionFeedbackUrlByUrn = (urn: string): string =>
  `${datasetComplianceSuggestionUrlByUrn(urn)}/feedback`;

/**
 * Returns the url for a dataset's export policy by urn
 * @param {string} urn
 * @return {string}
 */
export const datasetExportPolicyByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/exportpolicy`;

/**
 * Reads the dataset compliance policy by urn.
 * Resolves with a new compliance policy instance if remote response is ApiResponseStatus.NotFound
 * @param {string} urn the urn for the related dataset
 * @return {Promise<IReadComplianceResult>}
 */
export const readDatasetCompliance = (urn: string): Promise<IDatasetComplianceInfo> =>
  getJSON({ url: datasetComplianceUrl(urn) }).then(({ complianceInfo }): IDatasetComplianceInfo => complianceInfo);

/**
 * Reads the suggestions for a dataset compliance policy by urn
 * @param {string} urn
 * @return {Promise<IComplianceSuggestion>}
 */
export const readDatasetComplianceSuggestion = (urn: string): Promise<IDatasetComplianceSuggestionInfo> =>
  getJSON({ url: datasetComplianceSuggestionUrlByUrn(urn) }).then(
    ({ complianceSuggestion }): IDatasetComplianceSuggestionInfo => complianceSuggestion
  );

/**
 * Saves the suggestion feedback for a dataset when selected by the user
 * @param {string} urn the urn for the dataset with suggestions
 * @param {string | null} uid the suggestion uid
 * @param {SuggestionIntent} feedback indicator for acceptance or discarding a suggestion
 * @return {Promise<void>}
 */
export const saveDatasetComplianceSuggestionFeedbackByUrn = (
  urn: string,
  uid: string | null,
  feedback: SuggestionIntent
): Promise<void> => postJSON<void>({ url: datasetComplianceSuggestionFeedbackUrlByUrn(urn), data: { uid, feedback } });

/**
 * Persists the dataset compliance policy
 * @param {string} urn
 * @param {IComplianceInfo} complianceInfo
 * @return {Promise<void>}
 */
export const saveDatasetCompliance = (urn: string, complianceInfo: IDatasetComplianceInfo): Promise<void> =>
  postJSON({
    url: datasetComplianceUrl(urn),
    data: { ...complianceInfo, complianceType: null, compliancePurgeNote: null }
  });

/**
 * Defines the url endpoint for the list of dataset compliance data types and attributes
 * @type {string}
 */
const complianceDataTypesUrl = `${getListUrlRoot(ApiVersion.v2)}/compliance-data-types`;

/**
 * Requests the list of compliance data types and the related attributes
 * @returns {Promise<Array<IComplianceDataType>>}
 */
export const readComplianceDataTypes = (): Promise<Array<IComplianceDataType>> =>
  getJSON({ url: complianceDataTypesUrl }).then(
    ({ complianceDataTypes }): Array<IComplianceDataType> => complianceDataTypes
  );

/**
 * Constructs the url for a datasets retention policy
 * @param {string} urn the urn for the dataset
 * @return {string}
 */
const datasetRetentionUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/retention`;

/**
 * Fetches the list of retention policy for a dataset by urn
 * @param {string} urn urn for the dataset
 * @return {Promise<IDatasetRetentionPolicy>}
 */
export const readDatasetRetention = (urn: string): Promise<IDatasetRetentionPolicy> =>
  getJSON({ url: datasetRetentionUrlByUrn(urn) }).then(
    ({ retentionPolicy }): IDatasetRetentionPolicy => retentionPolicy
  );

/**
 * Persists the dataset retention policy remotely
 * @param {string} urn the urn of the dataset to save
 * @param {IDatasetRetention} retention the dataset retention policy to update
 * @return {Promise<IDatasetRetentionPolicy>}
 */
export const saveDatasetRetention = (
  urn: string,
  retention: Omit<IDatasetRetentionPolicy, 'modifiedBy' | 'modifiedTime'>
): Promise<IDatasetRetentionPolicy> =>
  postJSON({
    url: datasetRetentionUrlByUrn(urn),
    data: retention
  });

/**
 * Reads the export policy for a dataset by urn
 * @param urn
 * @return {Promise<IDatasetExportPolicy>}
 */
export const readDatasetExportPolicy = (urn: string): Promise<IDatasetExportPolicy> =>
  getJSON({ url: datasetExportPolicyByUrn(urn) }).then(({ exportPolicy }): IDatasetExportPolicy => exportPolicy);

export const saveDatasetExportPolicy = async (
  urn: string,
  exportPolicy: Omit<IDatasetExportPolicy, 'modifiedBy' | 'modifiedTime'>
): Promise<IDatasetExportPolicy> => {
  const response = (await postJSON({
    url: datasetExportPolicyByUrn(urn),
    data: exportPolicy
  })) as { exportPolicy: IDatasetExportPolicy };

  return response.exportPolicy;
};
