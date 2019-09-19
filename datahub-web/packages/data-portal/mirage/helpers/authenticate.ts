import { ApiStatus } from '@datahub/utils/api/shared';
import { Response, faker } from 'ember-cli-mirage';

type StringOrNullOrUndefined = string | null | void;

const textContentHeader = { 'Content-Type': 'text/plain; charset=utf-8' };

/**
 * Returns a config object for the config endpoint
 * @param {object} _schema the auth table / factory object
 * @param {requestBody} property on the request object passed in to mirage function handlers
 * @return {{status: ApiStatus, data: object}}
 */
const getAuth = (_schema: {}, { requestBody }: { requestBody: string }) => {
  const { username, password } = <{ username: StringOrNullOrUndefined; password: StringOrNullOrUndefined }>(
    JSON.parse(requestBody)
  );
  if (!password) {
    return new Response(400, textContentHeader, 'Missing or invalid [credentials]');
  }

  if (password === 'invalidPassword' || password === 'invalidPassword123456') {
    return new Response(401, textContentHeader, 'Invalid Password');
  }

  return {
    status: ApiStatus.OK,
    data: { username, uuid: faker.random.uuid() }
  };
};

export { getAuth };
