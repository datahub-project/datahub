import { getApiRoot, ApiStatus } from '@datahub/utils/api/shared';
import { getJSON } from '@datahub/utils/api/fetcher';

const currentUserUrl = `${getApiRoot()}/user/me`;

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
  pictureLink: string;
  userSetting: null | {
    defaultWatch: string;
    detailDefaultView: string;
  };
}

/**
 * Requests the currently logged in user and if the response is ok,
 * returns the user, otherwise throws
 */
export const currentUser = async (): Promise<IUser> => {
  const response = (await getJSON({ url: currentUserUrl })) as { user: IUser; status: ApiStatus };
  const { status = ApiStatus.FAILED, user } = response;

  if (status === ApiStatus.OK) {
    return user;
  }

  throw new Error(`Exception: ${status}`);
};
