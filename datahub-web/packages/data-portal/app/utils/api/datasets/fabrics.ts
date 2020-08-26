import { getJSON } from '@datahub/utils/api/fetcher';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';

/**
 * Constructs the url to retrieve fabrics for a dataset
 * @param {string} urn
 * @return {string}
 */
const datasetFabricsUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/fabrics`;

/**
 * Fetches the list of fabrics for a given dataset
 * @param {string} urn
 * @return {Promise<Array<Fabric>>}
 */
const readDatasetFabricsByUrn = (urn: string): Promise<Array<FabricType>> =>
  getJSON<Array<FabricType>>({ url: datasetFabricsUrlByUrn(urn) });

export { readDatasetFabricsByUrn };
