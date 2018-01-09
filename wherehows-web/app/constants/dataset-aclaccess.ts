import { capitalize } from '@ember/string';
import { IpageConcent, IpageState, IAclInfo, Iprincipal } from 'wherehows-web/typings/api/datasets/aclaccess';

/**
 * Defined the method to gnerate the static resource of ACL page based on the current user
 * @param {string} userName
 * @return {IpageConcent} pageContent
 */
const pageContent = (userName = ''): IpageConcent => {
  userName = capitalize(userName);
  return {
    success: {
      info: `${userName}, you have access to this data`,
      requestInfo: 'Congrats! Your request has been approved!',
      requestMessage: 'You now have a access to this data',
      classNameIcon: 'fa fa-check-circle-o fa-lg',
      classNameFont: 'acl-permission__success'
    },
    reject: {
      info: `${userName}, you currently do not have access to this dataset`,
      requestInfo: 'Sorry, you request has been denied by the system.',
      requestMessage: 'If you feel this is in error, contact acreqjests@linkedin.',
      classNameIcon: 'fa fa-ban fa-lg',
      classNameFont: 'acl-permission__reject'
    }
  };
};

/**
 * Defined the method to generate ACL page static content on all page state
 * @param {string} userName 
 * @return {IpageState} accessState
 */
const accessState = (userName: string): IpageState => {
  const content = pageContent(userName);
  return {
    hasAccess: {
      state: 'hasAccess',
      info: content.success.info,
      icon: content.success.classNameIcon,
      font: content.success.classNameFont
    },
    noAccess: {
      state: 'noAccess',
      info: content.reject.info,
      icon: content.reject.classNameIcon,
      font: content.reject.classNameFont,
      isLoadForm: true
    },
    getAccess: {
      state: 'getAccess',
      info: content.success.requestInfo,
      icon: content.success.classNameIcon,
      font: content.success.classNameFont,
      message: content.success.requestMessage
    },
    denyAccess: {
      state: 'denyAccess',
      info: content.reject.requestInfo,
      icon: content.reject.classNameIcon,
      font: content.reject.classNameFont,
      message: content.reject.requestMessage
    }
  };
};

/**
 * Defined a specific method 
 * to pick users info from ACL permission response and request response. 
 * @param {IAclInfo} permissionResponse 
 * @param {any} requestResponse 
 * @returns {Array<any>} pickList
 */

const pickList = (permissionResponse: IAclInfo, requestResponse: any) => {
  let result: Array<any> = [];
  if (permissionResponse && permissionResponse.body) {
    result = permissionResponse.body.map(item => item.tableItem);
  }
  if (requestResponse && requestResponse.hasOwnProperty('tableItem')) {
    result.push(requestResponse.tableItem);
  }
  return result;
};

/**
 * Defined a method to generate ACL post request body
 * @param {string} userName 
 * @param {string} requestReason
 * @returns {Iprincipal} getPrincipal
 */
const getPrincipal = (userName: string, requestReason: string): Iprincipal => {
  return {
    principal: `urn:li:userPrincipal:${userName}`,
    businessJustification: requestReason
  };
};

/**
 * Defined ACL authentication server address
 */
const queryAccessUrl = '/api/v1/acl';

/**
 *  For testing purpose, defined a mocked response when user is granted ACL permission
 */
const approvedResponseTesting = {
  principal: 'urn:li:userPrincipal:ABC',
  businessJustification: 'asdsd read',
  accessTypes: 'READ',
  tableItem: {
    userName: 'ABC',
    name: 'Solon Streich I',
    idType: 'USER',
    source: 'TY',
    modifiedTime: '2017-03-19T23:34:52.456Z',
    ownerShip: 'DataOwner'
  },
  id: 3
};

/**
 * For testing purpose, defined a method to mock ACL server response
 */
const accessInfoTesting = (permmision: boolean) => {
  return {
    isAccess: permmision,
    body: [approvedResponseTesting]
  };
};

export { pageContent, accessState, queryAccessUrl, pickList, accessInfoTesting, approvedResponseTesting, getPrincipal };
