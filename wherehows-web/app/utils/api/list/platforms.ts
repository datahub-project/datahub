import { IPlatformsResponse, IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { getListUrlRoot } from 'wherehows-web/utils/api/list/shared';

/**
 * Defines the url endpoint for the list of data platforms
 */
const platformsUrl = `${getListUrlRoot('v2')}/platforms`;

/**
 * Requests the list of data platforms and associated properties
 * @returns {Promise<Array<IDataPlatform>>}
 */
const readPlatforms = async (): Promise<Array<IDataPlatform>> => {
  const { platforms = [] } = await getJSON<IPlatformsResponse>({ url: platformsUrl });

  return platforms;
};

export { readPlatforms };
