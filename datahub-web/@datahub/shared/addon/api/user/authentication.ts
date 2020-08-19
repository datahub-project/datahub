import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { getJSON } from '@datahub/utils/api/fetcher';
import { ICorpUserInfo } from '@datahub/metadata-types/types/entity/person/person-entity';

const currentUserUrl = `${getApiRoot(ApiVersion.v1)}/user/me`;

/**
 * Requests the currently logged in user and returns that user
 */
export const currentUser = (): Promise<ICorpUserInfo> => getJSON({ url: currentUserUrl });
