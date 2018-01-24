import { getApiRoot } from 'wherehows-web/utils/api/shared';

/**
 * Defines the endpoint for datasets
 * @type {string}
 */
export const datasetsUrlRoot = `${getApiRoot()}/datasets`;

/**
 * Constructs a url to get a dataset with a given id
 * @param {number} id the id of the dataset
 * @return {string} the dataset url
 */
export const datasetUrlById = (id: number): string => `${datasetsUrlRoot}/${id}`;
