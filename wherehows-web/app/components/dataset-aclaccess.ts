import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { inject } from '@ember/service';
import { computed, getProperties, get, set } from '@ember/object';
import { task } from 'ember-concurrency';

import { getAclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import { accessState, pickList, getPrincipal, aclPageState } from 'wherehows-web/constants/dataset-aclaccess';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { IAclInfo, IAclUserInfo, IRequestResponse, IPageState } from 'wherehows-web/typings/api/datasets/aclaccess';

export default class DatasetAclAccess extends Component {
  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   * @memberOf DatasetAclAccess
   */
  notifications = <ComputedProperty<Notifications>>inject();

  /**
   * Define property binds to the textarea
   * @type {string}
   * @memberOf DatasetAclAccess
   */
  requestReason: string;

  /**
   * Define the property to show the user info
   * @type {string}
   * @memberOf DatasetAclAccess
   */
  currentUser: string;

  /**
   * Define the property to initialize the page
   * @type {IAclInfo}
   * @memberOf DatasetAclAccess
   */
  accessInfo: IAclInfo;

  /**
   *  Define the property to update the page when a user is granted permission.
   * @type {IRequestResponse}
   * @memberOf DatasetAclAccess
   */
  accessResponse: IRequestResponse;

  /**
   * Define the computed property to decide the page state.
   * The component has 5 states ['emptyState', 'hasAccess','noAccess','denyAccess','getAccess'].
   * @type {string}
   * @memberOf DatasetAclAccess
   */
  pageState: ComputedProperty<string> = computed('accessInfo', 'accessResponse', function(this: DatasetAclAccess) {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);

    const isLoadPage = accessInfo ? true : false;
    const hasAccess = accessInfo && accessInfo.isAccess ? true : false;
    const canAccess = accessResponse ? true : false;
    const idApproved = accessResponse && accessResponse.hasOwnProperty('isApproved') ? true : false;

    if (!isLoadPage) {
      return aclPageState.emptyState;
    } else if (hasAccess) {
      return aclPageState.hasAccess;
    } else if (!canAccess) {
      return aclPageState.noAccess;
    } else if (idApproved) {
      return aclPageState.denyAccess;
    }

    return aclPageState.getAccess;
  });

  /**
   * Define the computed property to pick authorized users information based on the property of accessInfo and accessResponse
   * @type Array<IAclUserInfo>
   * @memberOf DatasetAclAccess
   */
  users: ComputedProperty<Array<IAclUserInfo>> = computed('accessInfo', 'accessResponse', function(
    this: DatasetAclAccess
  ) {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);
    return pickList(accessInfo, accessResponse);
  });

  /**
   * Define the computed property to load page content based on pageState
   * @type { IPageState }
   * @memberOf DatasetAclAccess
   */
  state: ComputedProperty<IPageState> = computed('pageState', function(this: DatasetAclAccess) {
    const { currentUser, pageState } = getProperties(this, ['currentUser', 'pageState']);
    return accessState(currentUser)[pageState];
  });

  /**
   * Action to reset the request form
   * @memberOf DatasetAclAccess
   */
  resetForm(this: DatasetAclAccess) {
    set(this, 'requestReason', '');
  }

  /**
   * Action to request ACL access permission
   * @memberOf DatasetAclAccess
   */
  requestAccess = task(function*(this: DatasetAclAccess): IterableIterator<Promise<IRequestResponse>> {
    const { requestReason, currentUser } = getProperties(this, ['requestReason', 'currentUser']);
    const requestBody = getPrincipal(currentUser, requestReason);

    try {
      let response: IRequestResponse = yield getAclAccess(currentUser, requestBody);
      set(this, 'accessResponse', response);
    } catch (error) {
      get(this, 'notifications').notify(NotificationEvent.error, {
        content: `${error}, please report to acreqjests@linkedin`
      });
      throw `Request access error : ${error}`;
    }
  }).drop();

  actions = {
    /**
     * Action to cancel all tasks due to long time waiting
     */
    cancelTasks(this: DatasetAclAccess) {
      this.requestAccess.cancelAll();
    },

    /**
     * no-op, method for icon of "add suggested owner", it will be rewritten latter
     */
    addOwner(): void {}
  };
}
