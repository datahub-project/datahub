import { IPlatformsResponse, IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { ApiStatus } from 'wherehows-web/utils/api';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { listUrlRoot } from 'wherehows-web/utils/api/list/shared';

/**
 * Defines the url endpoint for the list of data platforms
 */
const platformsUrl = `${listUrlRoot}/platforms`;

/**
 * Requests the list of data platforms and associated properties
 * @returns {Promise<Array<IDataPlatform>>} 
 */
const readPlatforms = async (): Promise<Array<IDataPlatform>> => {
  const { status, platforms = [], msg } = await getJSON<IPlatformsResponse>({ url: platformsUrl });

  if (status === ApiStatus.OK) {
    if (!platforms.length) {
      throw new Error('No platforms found');
    }

    return platforms;
  }

  throw new Error(msg);
};

export { readPlatforms };
