import { putJSON } from '@datahub/utils/api/fetcher';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

/**
 * Returns the url for a dataset deprecation endpoint by urn
 * @param {string} urn
 * @return {string}
 */
const datasetDeprecationUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/deprecate`;

/**
 * Persists the changes to a datasets deprecation properties by urn
 * @param {string} urn
 * @param {boolean} deprecated
 * @param {string} deprecationNote
 * @param {Date | null} decommissionTime
 * @return {Promise<void>}
 */
export const updateDatasetDeprecationByUrn = (
  urn: string,
  deprecated: boolean,
  deprecationNote = '',
  decommissionTime: number | null
): Promise<void> =>
  putJSON<void>({
    url: datasetDeprecationUrlByUrn(urn),
    data: {
      deprecated,
      deprecationNote,
      decommissionTime
    }
  });
