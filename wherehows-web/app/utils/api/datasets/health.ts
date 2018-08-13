import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { IHealthScoreResponse, IDatasetHealth } from 'wherehows-web/typings/api/datasets/health';

/**
 * Returns the url for dataset metadata health by urn
 * @param urn
 * @return {string}
 */
const datasetHealthUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/health`;

export const readDatasetHealthByUrn = async (urn: string): Promise<IDatasetHealth> => {
  const { health } = await getJSON<IHealthScoreResponse>({ url: datasetHealthUrlByUrn(urn) });

  return health || { score: 0, validations: [] };
};
