import { capitalize } from '@ember/string';
import {
  IPageConcent,
  IPageState,
  IAclInfo,
  IPrincipal,
  IRequestResponse,
  IRequestAclApproved,
  IAclUserInfo
} from 'wherehows-web/typings/api/datasets/aclaccess';

/**
 * List of string values for ACL page state
 * @enum {string}
 */
enum aclPageState {
  emptyState = 'emptyState',
  hasAccess = 'hasAccess',
  getAccess = 'getAccess',
  noAccess = 'noAccess',
  denyAccess = 'denyAccess'
}

/**
 * Defined the method to gnerate the static resource of ACL page based on the current user
 * @param {string} userName
 * @return {IPageConcent} pageContent
 */
const pageContent = (userName = ''): IPageConcent => {
  userName = capitalize(userName);
  return {
    success: {
      info: `${userName}, you have access to this data`,
      requestInfo: 'Congrats! Your request has been approved!',
      requestMessage: 'You now have access to this data',
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
 * @return {IPageState} accessState
 */
const accessState = (userName: string): IPageState => {
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
 * @param {IRequestResponse} requestResponse
 * @returns {Array<IAclUserInfo>} pickList
 */
const pickList = (permissionResponse: IAclInfo, requestResponse: IRequestResponse): Array<IAclUserInfo> => {
  let result: Array<IAclUserInfo> = [];
  if (permissionResponse && permissionResponse.body) {
    result = permissionResponse.body.map(item => item.tableItem);
  }
  if (requestResponse && requestResponse.hasOwnProperty('tableItem')) {
    result.push((<IRequestAclApproved>requestResponse).tableItem);
  }
  return result;
};

/**
 * Defined a method to generate ACL post request body
 * @param {string} userName 
 * @param {string} requestReason
 * @returns {IPrincipal} getPrincipal
 */
const getPrincipal = (userName: string, requestReason: string): IPrincipal => {
  return {
    principal: `urn:li:userPrincipal:${userName}`,
    businessJustification: requestReason
  };
};

export { pageContent, accessState, pickList, getPrincipal, aclPageState };
