import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { getJSON } from '@datahub/utils/api/fetcher';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { encodeUrn } from '@datahub/utils/validators/urn';

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
  getJSON<Array<FabricType>>({ url: datasetFabricsUrlByUrn(encodeUrn(urn)) });

export { readDatasetFabricsByUrn };
