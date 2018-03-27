import { IAccessControlEntry } from 'wherehows-web/typings/api/datasets/aclaccess';
import { arrayFilter } from 'wherehows-web/utils/array';

/**
 * Checks if the username matches the principal attribute in an IAccessControlEntry object,
 * returns a closure function that accepts the an IAccessControlEntry object
 * @param {string} userName the principal to match against
 * @returns {(o: IAccessControlEntry) => boolean}
 */
const isUserAccessControlEntry = (userName: string) => ({ principal }: IAccessControlEntry) => principal === userName;

/**
 * Checks if the username has an entry in the acl,
 * returns a closure function that accepts a list of acl
 * @param {string} userName the principal to match against
 * @returns {(a: Array<IAccessControlEntry>) => Array<IAccessControlEntry>}
 */
const isUserInAcl = (userName: string) => (acls: Array<IAccessControlEntry>): Array<IAccessControlEntry> =>
  arrayFilter(isUserAccessControlEntry(userName))(acls);

export { isUserInAcl };
