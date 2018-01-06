import Component from '@ember/component';
import { computed, getProperties, setProperties, get, set } from '@ember/object';
import { requestAclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import { accessState, pickList } from 'wherehows-web/constants/dataset-aclaccess';

export default Component.extend({
  /**
   * Define property to bind textarea
   * @type {string}
   */
  requestReason: '',

  /**
   * Define the property for the initial page  
   * @type {object} 
   */
  accessInfo: null,

  /**
   * Define the property for reloading the page when a user applies for permission.
   * @type {object} 
   */
  accessResponse: null,

  /**
   * Define the property to identify user 
   * @type {string} 
   */
  currentUser: null,

  /**
   * Define the property for loading spinner component
   * @type {boolean} 
   */
  isLoadSpinner: false,

  /**
   * Define the property for deciding component state. The component has 5 states.
   * @type {string}
   */
  pageState: computed('accessInfo', 'accessResponse', function() {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);

    if (accessInfo) {
      if (accessInfo.isAccess) {
        return 'hasAcess';
      } else {
        if (accessResponse) {
          if (accessResponse.hasOwnProperty('isApproved')) {
            return 'denyAccess';
          }
          return 'getAccess';
        }
        return 'noAccess';
      }
    }
    return null;
  }),

  /**
   * Define the property for picking authenticate users based on accessInfo and accessResponse
   * @type {string}
   */
  users: computed('accessInfo', 'accessResponse', function() {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);
    return pickList(accessInfo, accessResponse);
  }),

  /**
   * Define the property for loading state information object which contains all info on the page
   * @type {object}
   */
  state: computed('pageState', function() {
    const { currentUser, pageState } = getProperties(this, ['currentUser', 'pageState']);
    return accessState(currentUser)[pageState];
  }),

  /**
   * Define the property for loading the request form
   * @type {object}
   */
  isLoadForm: computed('pageState', function() {
    return get(this, 'pageState') === 'noAccess';
  }),

  /**
   * Reset the form
   * @param {} none
   */
  resetForm() {
    set(this, 'requestReason', '');
  },

  actions: {
    /**
     * Handle request form to change page UI
     */
    requestAccess() {
      const { requestReason, currentUser } = getProperties(this, ['requestReason', 'currentUser']);
      const data = {
        principal: `urn:li:userPrincipal:${currentUser}`,
        businessJustification: requestReason
      };

      set(this, 'isLoadSpinner', true);

      Promise.resolve(requestAclAccess(currentUser, data))
        .then(response => {
          setProperties(this, {
            accessResponse: response,
            isLoadSpinner: false
          });
        })
        .catch(error => {
          throw `It is request access error : ${error}`;
        });
    },

    /**
     * close the loading spinner
     */
    resetSpinner() {
      set(this, 'isLoadSpinner', false);
    },

    /**
     *  reset request form
     */
    cancelRequest() {
      this.resetForm();
    },

    /**
     * Handle request form to add more user
     */
    addOwner() {}
  }
});
