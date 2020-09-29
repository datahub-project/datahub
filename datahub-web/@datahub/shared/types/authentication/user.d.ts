import { ApiStatus } from '@datahub/utils/api/shared';
import { IUser } from '@datahub/metadata-types/types/common/user';

/**
 * Describes the current user endpoint response
 */
export interface ICurrentUserResponse {
  user: IUser;
  status: ApiStatus;
}

export interface IAuthenticationData {
  username: string;
  uuid: string;
}

/**
 * Describes the return shape for the authenticate endpoint
 */
export interface IAuthenticateResponse {
  data: IAuthenticationData;
  status: ApiStatus;
}
