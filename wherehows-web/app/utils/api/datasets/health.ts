import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { IHealthScoreResponse, IDatasetHealth } from 'wherehows-web/typings/api/datasets/health';

/**
 * Returns the url for dataset metadata health by urn
 * @param urn
 * @return {string}
 */
const datasetHealthUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/health`;

/**
 * Constant for formatting of the validator string
 * @type {string}
 */
const validatorFormat = 'com.linkedin.metadata.validators';

/**
 * Useful for removing extraneous information outside of category
 * @type {RegExp}
 */
const validationRegex = new RegExp(`${validatorFormat}.|Validator`, 'gi');

/**
 * Given a string in the format 'com.linkedin.metadata.validators.[Category]Validator', extract
 * Category and return it.
 * @param validator - Given validator string
 * @returns {string}
 */
export const getCategory = (validator: string) => validator.replace(validationRegex, '');

export const readDatasetHealthByUrn = async (urn: string): Promise<IDatasetHealth> => {
  const defaultResponse = { score: 0, validations: [] };

  try {
    const { health } = await getJSON<IHealthScoreResponse>({ url: datasetHealthUrlByUrn(urn) });

    return health;
  } catch {
    return defaultResponse;
  }
};
