import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { inject } from '@ember/service';
import { computed, getProperties, get, set } from '@ember/object';
import { task } from 'ember-concurrency';

import { getAclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import { accessState, pickList, getPrincipal } from 'wherehows-web/constants/dataset-aclaccess';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { IAclInfo, IRequestAclReject, IRequestAclApproved } from 'wherehows-web/typings/api/datasets/aclaccess';

export default class DatsetAclAccess extends Component {
  constructor() {
    super(...arguments);
    this.resetForm = this.resetForm.bind(this);
    this.actions.cancelTasks = this.actions.cancelTasks.bind(this);
    this.actions.addOwner = this.actions.addOwner.bind(this);
  }
  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   * @memberOf DatsetAclAccess
   */
  notifications = <ComputedProperty<Notifications>>inject();

  /**
   * Define property binds to the textarea
   * @type {string}
   * @memberOf DatsetAclAccess
   */
  requestReason: string;

  /**
   * Define the property to show the user info
   * @type {string}
   * @memberOf DatsetAclAccess
   */
  currentUser: string;

  /**
   * Define the property to initialize the page 
   * @type {IAclInfo}
   * @memberOf DatsetAclAccess
   */
  accessInfo: IAclInfo;

  /**
   *  Define the property to update the page when a user is granted permission.
   * @type {IRequestAclReject | IRequestAclApproved}
   * @memberOf DatsetAclAccess
   */
  accessResponse: IRequestAclReject | IRequestAclApproved;

  /**
   * Define the computed property to decide the page state. 
   * The component has 5 states ['emptyState', 'hasAccess','noAccess','denyAccess','getAccess'].
   * @type {string}
   * @memberOf DatsetAclAccess
   */
  pageState: ComputedProperty<string> = computed('accessInfo', 'accessResponse', function(this: DatsetAclAccess) {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);

    const isLoadPage = accessInfo ? true : false;
    const hasAccess = accessInfo && accessInfo.isAccess ? true : false;
    const canAccess = accessResponse ? true : false;
    const idApproved = accessResponse && accessResponse.hasOwnProperty('isApproved') ? true : false;

    if (!isLoadPage) {
      return 'emptyState';
    } else if (hasAccess) {
      return 'hasAccess';
    } else if (!canAccess) {
      return 'noAccess';
    } else if (idApproved) {
      return 'denyAccess';
    }

    return 'getAccess';
  });

  /**
   * Define the computed property to pick authorized users information based on the property of accessInfo and accessResponse
   * @type {string}
   * @memberOf DatsetAclAccess
   */
  users: ComputedProperty<Array<any>> = computed('accessInfo', 'accessResponse', function(this: DatsetAclAccess) {
    const { accessInfo, accessResponse } = getProperties(this, ['accessInfo', 'accessResponse']);
    return pickList(accessInfo, accessResponse);
  });

  /**
   * Define the computed property to load page content based on pageState
   * @type {object}
   * @memberOf DatsetAclAccess
   */
  state: ComputedProperty<object> = computed('pageState', function(this: DatsetAclAccess) {
    const { currentUser, pageState } = getProperties(this, ['currentUser', 'pageState']);
    return accessState(currentUser)[pageState];
  });

  /**
   * Action to reset the request form
   * @memberOf DatsetAclAccess
   */
  resetForm(this: DatsetAclAccess) {
    set(this, 'requestReason', '');
  }

  /**
   * Action to request ACL access permission
   * @memberOf DatsetAclAccess
   */
  requestAccess = task(function*(this: DatsetAclAccess): IterableIterator<Promise<any>> {
    const { requestReason, currentUser } = getProperties(this, ['requestReason', 'currentUser']);
    const requestBody = getPrincipal(currentUser, requestReason);

    try {
      let response = yield getAclAccess(currentUser, requestBody);
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
    cancelTasks(this: DatsetAclAccess) {
      this.requestAccess.cancelAll();
    },

    /**
     * no-op, method for icon of "add suggested owner", it will be rewritten latter
     */
    addOwner(): void {}
  };
}
