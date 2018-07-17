import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';
import { Fabric } from 'wherehows-web/constants';

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
const readDatasetFabricsByUrn = (urn: string): Promise<Array<Fabric>> =>
  getJSON<Array<Fabric>>({ url: datasetFabricsUrlByUrn(encodeUrn(urn)) });

export { readDatasetFabricsByUrn };
