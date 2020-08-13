import { getJSON } from '@datahub/utils/api/fetcher';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';
import { isNotFoundApiError } from '@datahub/utils/api/shared';

const datasetGroupsByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/groups`;

/**
 * Fetches the dataset groups from the mid tier for the given URN
 * @param {string} urn The URN of the dataset
 * @return {Promise<Array<{urn:string}>}
 */
export const readDatasetGroups = (urn: string): Promise<Array<{ urn: string }>> => {
  try {
    return getJSON({ url: datasetGroupsByUrn(urn) });
  } catch (e) {
    // In the case a 404 is encountered. We return an empty list to the UI , so that the 404 is gracefully handled.
    if (isNotFoundApiError(e)) {
      return Promise.resolve([]);
    }
    throw e;
  }
};
