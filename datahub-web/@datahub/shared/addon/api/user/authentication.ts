import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { getJSON } from '@datahub/utils/api/fetcher';
import { ICorpUserInfo } from '@datahub/metadata-types/types/entity/person/person-entity';
import { IUser } from '@datahub/metadata-types/types/common/user';

const currentUserUrl = `${getApiRoot(ApiVersion.v2)}/user/me`;

/**
 * Requests the currently logged in user and returns that user
 */
export const currentUser = (): Promise<ICorpUserInfo> => getJSON({ url: currentUserUrl });

/**
 * Because the open source midtier is currently lagging behind, we need to have a handler for the previous /user/me
 * endpoint as otherwise it causes an issue with logging in on the open source frontend
 */
export const currentUserDeprecated = (): Promise<IUser> =>
  getJSON({ url: `${getApiRoot(ApiVersion.v1)}/user/me` }).then(({ user }: { user: IUser }): IUser => user);
