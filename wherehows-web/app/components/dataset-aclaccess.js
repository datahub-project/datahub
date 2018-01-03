import Component from '@ember/component';
import { inject } from '@ember/service';
import { computed, getProperties, get, set, trySet } from '@ember/object';

import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';
import { requestAclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';

import { baseCommentEditorOptions } from 'wherehows-web/constants';

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

export default Component.extend({
  notifications: inject(),
  requestReason: '',
  accessInfo: null,
  accessResponse: null,
  currentUser: null,
  isUserTyping: true,
  pageState: computed('accessInfo', 'accessResponse', function() {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);

    if (accessInfo) {
      if (accessInfo.isAccess) {
        return 'hasAcess';
      } else {
        if (accessResponse) {
          if (accessResponse.hasOwnProperty('isApproved')) {
            return 'denyAccess';
          } else {
            return 'getAccess';
          }
        } else {
          return 'noAccess';
        }
      }
    }
    return null;
  }),

  users: computed('accessInfo', 'accessResponse', function() {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);
    let aclList;

    if (accessInfo && accessInfo.body) {
      aclList = aclList || _.map(accessInfo.body, item => item.tableItem);
    }
    if (accessResponse && accessResponse.hasOwnProperty('tableItem')) {
      aclList.push(accessResponse.tableItem);
    }
    return aclList;
  }),

  state: computed('pageState', function() {
    const { currentUser, pageState } = getProperties(this, ['currentUser', 'pageState']);
    return accessState(currentUser)[pageState];
  }),

  isLoadForm: computed('pageState', function() {
    return get(this, 'pageState') === 'noAccess';
  }),

  resetForm() {
    set(this, 'requestReason', '');
  },

  setState(key, value) {
    set(this, key, value);
  },

  actions: {
    // async
    async click() {
      //for testing choose email.
      const choseEmail = get(this, 'isLoading') ? 0 : 1;
      const email = dummyLDAP[choseEmail];

      set(this, 'currentUser', email);
      const url = `${queryAccessUrl}?LDAP=${email}`;

      const response = await getJSON({
        url: url
      });

      // console.log('email', get(this, 'currentUser'));
      // console.log('response', response);
      set(this, 'accessInfo', response);
      this.toggleProperty('isLoading');
    },

    requestAccess() {
      const { requestReason, currentUser } = getProperties(this, ['requestReason', 'currentUser']);
      // const inputComment = get(this, 'requestReason');
      // const currentUser = get(this, 'currentUser');
      const data = {
        principal: `urn:li:userPrincipal:${currentUser}`,
        businessJustification: requestReason
      };

      Promise.resolve(requestAclAccess(currentUser, data))
        .then(response => {
          this.setState('accessResponse', response);
        })
        .catch(error => {
          throw `It is request access error : ${error}`;
        });
    },
    cancelRequest() {
      this.resetForm();
    },
    addOwner() {
      console.log('add owner');
    },
    notification() {
      get(this, 'notifications').notify(NotificationEvent.confirm, {
        header: 'Delete comment',
        content: 'Are you sure you want to delete this comment?'
      });
    }
  }
});
