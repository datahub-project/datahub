import { getListUrlRoot } from '@datahub/data-models/api/dataset/shared/lists';
import { ApiVersion } from '@datahub/utils/api/shared';
import { getJSON } from '@datahub/utils/api/fetcher';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';

/**
 * Defines the url endpoint for the list of dataset compliance data types and attributes
 * @type {string}
 */
const platformsUrl = `${getListUrlRoot(ApiVersion.v2)}/platforms`;

/**
 * Requests the list of compliance data types and the related attributes
 * @returns {Promise<Array<IComplianceDataType>>}
 */
export const readDataPlatforms = (): Promise<Array<IDataPlatform>> =>
  getJSON({ url: platformsUrl })
    .then(({ platforms }) => platforms || [])
    .catch(() => []);
