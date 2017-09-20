import { ApiStatus } from 'wherehows-web/utils/api';

/**
 * Describes the interface for the user json returned
 * from the current user endpoint
 */
export interface IUser {
  departmentNum: number;
  email: string;
  id: number;
  name: string;
  userName: string;
  userSetting: null | {
    defaultWatch: string;
    detailDefaultView: string;
  };
}

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
