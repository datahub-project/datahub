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

/**
 * Defined a method to array from two object
 * @param {*} objFirst 
 * @param {*} objSecond 
 * @returns [{*}] Array
 */
const pickList = (objFirst = {}, objSecond = {}) => {
  let result;
  if (objFirst && objFirst.body) {
    result = result || _.map(objFirst.body, item => item.tableItem);
  }
  if (objSecond && objSecond.hasOwnProperty('tableItem')) {
    result.push(objSecond.tableItem);
  }
  return result;
};

/**
 * Testing data
 */
const queryAccessUrl = 'http://localhost:3000/elements/';

const dummyLDAP = ['Mitchell_Rath', 'ABC', 'Juwan.Simonis', 'Gust.Tillman45', 'Tessie.Smitham59'];

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

const accessInfoTesting = permmision => {
  return {
    isAccess: permmision,
    body: [approvedResponseTesting]
  };
};

export { pageContent, accessState, queryAccessUrl, dummyLDAP, pickList, accessInfoTesting, approvedResponseTesting };
