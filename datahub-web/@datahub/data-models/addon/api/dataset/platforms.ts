import { getListUrlRoot } from '@datahub/data-models/api/dataset/shared/lists';
import { ApiVersion } from '@datahub/utils/api/shared';
import { getJSON } from '@datahub/utils/api/fetcher';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';

const platformsUrl = `${getListUrlRoot(ApiVersion.v2)}/platforms`;

export const readDataPlatforms = (): Promise<Array<IDataPlatform>> =>
  getJSON({ url: platformsUrl }).then(({ platforms }) => platforms);
