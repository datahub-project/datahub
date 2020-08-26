import { ApiStatus } from '@datahub/utils/api/shared';
import { Response, faker } from 'ember-cli-mirage';

type StringOrNullOrUndefined = string | null | void;

/**
 * Returns a config object for the config endpoint
 * @param {object} _schema the auth table / factory object
 * @param {requestBody} property on the request object passed in to mirage function handlers
 */
export const getAuth = (
  _schema: {},
  { requestBody }: { requestBody: string }
): Response | { status: ApiStatus; data: object } => {
  const { username, password } = JSON.parse(requestBody) as {
    username: StringOrNullOrUndefined;
    password: StringOrNullOrUndefined;
  };

  if (!password) {
    return new Response(
      400,
      {},
      {
        status: ApiStatus.FAILED,
        msg: 'Missing or invalid [credentials]'
      }
    );
  }

  if (password === 'invalidPassword' || password === 'invalidPassword123456' || password === 'validPasswordWithNoVip') {
    return new Response(
      400,
      {},
      {
        status: ApiStatus.FAILED,
        msg: 'Invalid Password'
      }
    );
  }

  return {
    status: ApiStatus.OK,
    data: { username, uuid: faker.random.uuid() }
  };
};
