// import Ember from 'ember';
// const { $: { getJSON } } = Ember;

import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';

// TODO: Defined check current user can access ACL tab and ACL users list

/**
 * Defined base URL for acl server 
 * @type {string}
 */
const aclUsersList = 'http://localhost:3000/elements/';

/**
 * Defined URL for checking current user accessibility
 * @param {string} userName the current user name
 * @return {string} URL check current user accessibility
 */

const checkUserAccess = userName => `${aclUsersList}?LDAP=${userName}`;

/**
 *  Defined the method for checking current user ACL accessibility
 * @param {string} userName 
 * @return {Promise object} 
 */

const checkAclAccess = async userName => {
  return await getJSON({ url: checkUserAccess(userName) });
};

/**
 *  Defined the method for requesting ACL accessibility
 * @param {string} userName 
 * @return {Promise object} 
 */
const requestAclAccess = (userName, data) => {
  return postJSON({
    url: checkUserAccess(userName),
    data
  });
};

export { checkAclAccess, requestAclAccess };
