import { datasetUrlRoot } from '@datahub/data-models/api/dataset/dataset';
import { ApiVersion } from '@datahub/utils/api/shared';

/**
 * Constructs a url to get a dataset with a given id
 * @param {number} id the id of the dataset
 * @return {string} the dataset url
 */
export const datasetUrlById = (id: number): string => `${datasetUrlRoot(ApiVersion.v1)}/${id}`;
