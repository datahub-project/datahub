import _ from 'lodash';

/**
 * Defined the method returns the page static content
 * @param {string} userName
 * @return {Object} pageContent
 */
const pageContent = userName => {
  userName = _.capitalize(userName);
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
 * Defined the method returns the page state
 * @param {*} userName 
 * @return {Object} accessState
 */
const accessState = userName => {
  const content = pageContent(userName);
  return {
    hasAcess: {
      state: 'hasAcess',
      info: content.success.info,
      icon: content.success.classNameIcon,
      font: content.success.classNameFont
    },
    noAccess: {
      state: 'noAccess',
      info: content.reject.info,
      icon: content.reject.classNameIcon,
      font: content.reject.classNameFont
    },
    getAccess: {
      state: 'getAccess',
      info: content.success.requestInfo,
      message: content.success.requestMessage,
      icon: content.success.classNameIcon,
      font: content.success.classNameFont
    },
    denyAccess: {
      state: 'denyAccess',
      info: content.reject.requestInfo,
      message: content.reject.requestMessage,
      icon: content.reject.classNameIcon,
      font: content.reject.classNameFont
    }
  };
};

const queryAccessUrl = 'http://localhost:3000/elements/';

const dummyLDAP = ['Mitchell_Rath', 'ABC', 'Juwan.Simonis', 'Gust.Tillman45', 'Tessie.Smitham59'];

export { pageContent, accessState, queryAccessUrl, dummyLDAP };
