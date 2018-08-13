import Service from '@ember/service';
import { getUserEntities } from 'wherehows-web/utils/api/datasets/owners';
import { IPartyEntity, IPartyProps } from 'wherehows-web/typings/api/datasets/party-entities';

/**
 * Takes a userNameQuery query and find userNames that match by starting with
 *  the pattern
 * @param {string} userNameQuery pattern to search for
 * @param {Function} _syncResults callback
 * @param {Function} asyncResults callback
 * @return {Promise<void>}
 */
const ldapResolver = async (
  userNameQuery: string,
  _syncResults: Function,
  asyncResults: (results: Array<string>) => void
): Promise<void> => {
  const ldapRegex = new RegExp(`^${userNameQuery}.*`, 'i');
  const { userEntitiesSource = [] }: IPartyProps = await getUserEntities();

  asyncResults(userEntitiesSource.filter((entity: string) => ldapRegex.test(entity)));
};

/**
 * For a given userName, find the userEntity object that contains the userName
 * @param {string} userName the unique userName
 * @return {Promise<IPartyEntity>} resolves with the userEntity or null otherwise
 */
const getPartyEntityWithUserName = (userName: string): Promise<IPartyEntity | null> =>
  getUserEntities().then(
    ({ userEntities }: IPartyProps) => userEntities.find(({ label }: { label: string }) => label === userName) || null
  );

export default class UserLookup extends Service {
  getPartyEntityWithUserName = getPartyEntityWithUserName;
  userNamesResolver = ldapResolver;
  fetchUserNames = getUserEntities;
}
