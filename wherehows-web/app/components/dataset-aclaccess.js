import Component from '@ember/component';
import { inject } from '@ember/service';
import { computed, get, set } from '@ember/object';
import { getJSON, postJSON } from 'wherehows-web/utils/api/fetcher';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';

import { currentUser as cuser } from 'wherehows-web/utils/api/authentication';
import { aclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import _ from 'lodash';

let pageContent = userName => {
  return {
    success: {
      info: `${userName}, you have access to this data`,
      requestInfo: 'Congrats! Your request has been approved!',
      requestMessage: 'You now have a access to this data',
      classNameIcon: 'fa fa-check-circle-o fa-lg',
      classNameFont: 'acl-access__success'
    },
    reject: {
      info: `${userName}, you currently do not have access to this dataset`,
      requestInfo: 'Sorry, you request has been denied by the system.',
      requestMessage: 'If you feel this is in error, contact acreqjests@linkedin.',
      classNameIcon: 'fa fa-ban fa-lg',
      classNameFont: 'acl-access__reject'
    }
  };
};

let accessState = userName => {
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
  // users: null,
  requestAccessReason: '',
  //new atitecture
  accessInfo: null,
  accessResponse: null,
  currentUser: null,
  pageState: computed('accessInfo', 'accessResponse', function() {
    let accessInfo = get(this, 'accessInfo');
    let accessResponse = get(this, 'accessResponse');

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

  state: computed('pageState', function() {
    let userName = get(this, 'currentUser');
    let pageState = get(this, 'pageState');
    return accessState(userName)[pageState];
  }),

  // change state to pageState
  isLoadForm: computed('pageState', function() {
    return get(this, 'pageState') === 'noAccess';
  }),

  resetForm() {
    set(this, 'requestAccessReason', '');
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

      // const res2 = await aclAccess(email);
      // const res3 = aclAccess(email).then(function(value) {
      //   return value;
      // });

      // console.log('res2', res2);
      // console.log('res3', res3);
      // this is work
      Promise.resolve(aclAccess(email)).then(value => {
        console.log('this is value', value);
      });

      set(this, 'accessInfo', response);

      const responseBody = response.body;
      const aclList = _.map(responseBody, item => item.tableItem);

      set(this, 'users', aclList);
      this.toggleProperty('isLoading');

      console.log('this is test propers', get(this, 'testProps'));

      //test username
      // const aboutme = await getJSON({
      //   url: 'http://localhost:4200/api/v1/user/me'
      // });
      const aboutme = await cuser();
      console.log('about Me', aboutme);
    },

    async sendRequest() {
      const inputComment = get(this, 'requestAccessReason');
      const currentUser = get(this, 'currentUser');
      const response = await postJSON({
        url: queryAccessUrl,
        data: {
          principal: `urn:li:userPrincipal:${currentUser}`,
          businessJustification: inputComment
        }
      });

      set(this, 'accessResponse', response);
      this.resetForm();
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
