import { ApiRoot } from 'wherehows-web/utils/api/shared';

/**
 * Defines the endpoint for datasets
 * @type {string}
 */
export const datasetsUrlRoot = `${ApiRoot}/datasets`;

/**
 * Constructs a url to get a dataset with a given id
 * @param {number} id the id of the dataset
 * @return {string} the dataset url
 */
export const datasetUrlById = (id: number): string => `${datasetsUrlRoot}/${id}`;

/**
 * Constructs a url to get a dataset id given a dataset urn
 * @param {string} urn
 * @return {string}
 */
export const datasetIdTranslationUrlByUrn = (urn: string): string => {
  return `${datasetsUrlRoot}/idfromurn/${encodeURIComponent(urn)}`;
};
