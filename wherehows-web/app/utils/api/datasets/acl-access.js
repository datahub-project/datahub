import { currentUser } from 'wherehows-web/utils/api/authentication';
// import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';
import { ApiStatus } from 'wherehows-web/utils/api';
const { $: { getJSON, post, ajax } } = Ember;

// TODO: Defined check current user can access ACL tab and ACL users list

/**
 * Defined URL for check current user accessibility
 * @type {string}
 * @param {string} userName the current user name
 * @return {string} URL check current user accessibility
 */
const aclUsersList = 'http://localhost:3000/elements/';
const checkUserAccess = userName => `${aclUsersList}?LDAP=${userName}`;

/**
 *  
 */

// const aclAccess = async userName => {
//   return getJSON(checkUserAccess(userName));
// };

const aclAccess = async userName => {
  const response = await Promise.resolve(getJSON(checkUserAccess(userName)));
  //   const { status } = response;', status
  console.log('this i response', response);

  return response;
};

export { aclAccess };
