import Component from '@ember/component';
import { inject } from '@ember/service';
import { computed, getProperties, get, set } from '@ember/object';

import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { requestAclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';

import { accessState, queryAccessUrl, dummyLDAP, pickList } from 'wherehows-web/constants/dataset-aclaccess';

export default Component.extend({
  notifications: inject(),
  requestReason: '',
  accessInfo: null,
  accessResponse: null,
  currentUser: null,
  isLoadSpinner: false,

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
    return pickList(accessInfo, accessResponse);
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

  actions: {
    // async
    async click() {
      //for testing choose email.
      const choseEmail = get(this, 'isLoading') ? 0 : 1;
      const email = dummyLDAP[choseEmail];

      set(this, 'currentUser', email);
      console.log('email', get(this, 'currentUser'));
      const url = `${queryAccessUrl}?LDAP=${email}`;

      const response = await getJSON({
        url: url
      });

      // console.log('email', get(this, 'currentUser'));
      // console.log('response', response);
      set(this, 'accessInfo', response);

      // this.set('accessInfo', response);
      this.toggleProperty('isLoading');
    },

    requestAccess() {
      const { requestReason, currentUser } = getProperties(this, ['requestReason', 'currentUser']);
      const data = {
        principal: `urn:li:userPrincipal:${currentUser}`,
        businessJustification: requestReason
      };

      set(this, 'isLoadSpinner', true);

      Promise.resolve(requestAclAccess(currentUser, data))
        .then(response => {
          set(this, 'accessResponse', response);
          // set(this, 'isLoadSpinner', false);
        })
        .catch(error => {
          throw `It is request access error : ${error}`;
        });

      console.log('parent isloadspinner', get(this, 'isLoadSpinner'));

      // get(this, 'notifications').notify(NotificationEvent.confirm, {
      //   header: 'Delete comment',
      //   content: 'Are you sure you want to delete this comment?'
      // });
    },

    resetSpinner() {
      console.log('reset spinner');
      set(this, 'isLoadSpinner', false);
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
