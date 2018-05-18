import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { IReadComplianceResult, readDatasetComplianceByUrn } from 'wherehows-web/utils/api/datasets/compliance';
import { IUpstreamWithComplianceMetadata } from 'wherehows-web/typings/app/datasets/lineage';
import { arrayMap } from 'wherehows-web/utils/array';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';

/**
 * Maps a dataset urn/uri, other props with a property indicating if the dataset compliance policy exists
 * @param {(Pick<IDatasetView, 'uri' | 'nativeName'>)} {
 *   uri,
 *   nativeName
 * }
 * @returns {Promise<IUpstreamWithComplianceMetadata>}
 */
const datasetWithComplianceMetadata = async ({
  uri,
  nativeName
}: Pick<IDatasetView, 'uri' | 'nativeName'>): Promise<IUpstreamWithComplianceMetadata> => {
  const { isNewComplianceInfo }: IReadComplianceResult = await readDatasetComplianceByUrn(encodeUrn(uri));
  return { urn: uri, hasCompliance: !isNewComplianceInfo, nativeName };
};

/**
 * Takes a list of datasets and maps into a list of IUpstreamWithComplianceMetadata objects
 * @param {Array<IDatasetView>} datasets
 * @returns {Array<Promise<IUpstreamWithComplianceMetadata>>}
 */
const datasetsWithComplianceMetadata = (
  datasets: Array<IDatasetView>
): Array<Promise<IUpstreamWithComplianceMetadata>> => arrayMap(datasetWithComplianceMetadata)(datasets);

export { datasetsWithComplianceMetadata };
