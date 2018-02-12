import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';
import { IPrincipal, IRequestResponse } from 'wherehows-web/typings/api/datasets/aclaccess';

/**
 * Defined ACL authentication server address
 */
const queryAccessUrl = '/api/v1/acl';

/**
 * Defined ACL authentication server URL
 * @param {string} userName 
 * @return {string} aclAuthURL
 */
const aclAuthURL = (userName: string): string => `${queryAccessUrl}?LDAP=${userName}`;

/**
 *  Defined the method to check a user ACL authorization status
 * @param {string} userName 
 * @return {Promise<any>} checkAclAccess
 */
const checkAclAccess = async (userName: string): Promise<any> => {
  return await getJSON({ url: aclAuthURL(userName) });
};

/**
 *  Defined the method to request ACL page permission
 * @param {string} userName
 * @param {IPrincipal} data 
 * @return {Promise<IRequestResponse>} getAclAccess
 */
const getAclAccess = (userName: string, data: IPrincipal): Promise<IRequestResponse> => {
  return postJSON({
    url: aclAuthURL(userName),
    data
  });
};

export { checkAclAccess, getAclAccess };
