import Component from '@ember/component';
import { get, set, getProperties, computed } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { TaskInstance, TaskProperty } from 'ember-concurrency';
import { action } from 'ember-decorators/object';
import DatasetAclAccessContainer from 'wherehows-web/components/datasets/containers/dataset-acl-access';
import { IAccessControlAccessTypeOption } from 'wherehows-web/typings/api/datasets/aclaccess';
import { getDefaultRequestAccessControlEntry } from 'wherehows-web/utils/datasets/acl-access';

export default class DatasetAclAccess extends Component {
  /**
   * The currently logged in user is listed on the related datasets acl
   * @type {boolean}
   * @memberof DatasetAclAccess
   */
  readonly userHasAclAccess: boolean;

  /**
   * Currently selected date
   * @type {Date}
   * @memberof DatasetAclAccess
   */
  selectedDate: Date = new Date();

  /**
   * Date around which the calendar is centered
   * @type {Date}
   * @memberof DatasetAclAccess
   */
  centeredDate: Date = this.selectedDate;

  /**
   * The earliest date a user can select as an expiration date
   * @type {Date}
   * @memberof DatasetAclAccess
   */
  minSelectableExpirationDate: Date = new Date(Date.now() + 24 * 60 * 60 * 1000);

  /**
   * External action invoked on change to access request access type
   * @type {(option: IAccessControlAccessTypeOption) => void}
   */
  accessTypeDidChange: (option: IAccessControlAccessTypeOption) => void;

  /**
   * External action invoked on change to expiration date
   * @type {(date: Date) => void}
   */
  expiresAtDidChange: (date: Date) => void;

  /**
   * External task to remove the logged in user from the related dataset's acl
   * @memberof DatasetAclAccess
   */
  removeAccessTask: TaskProperty<Promise<void>> & { perform: (a?: {} | undefined) => TaskInstance<Promise<void>> };

  /**
   * External task reference to the last request for acl entry aliases `requestAccessAndCheckAccessTask`, if exists
   * @type {ComputedProperty<TaskInstance<DatasetAclAccessContainer.requestAccessAndCheckAccessTask>>}
   * @memberof DatasetAclAccess
   */
  lastAccessRequestTask: ComputedProperty<TaskInstance<DatasetAclAccessContainer['requestAccessAndCheckAccessTask']>>;

  /**
   * Resolves to true if the last request for access results in an error and the logged in user,
   * does not have access. Used to indicate that the request may have been denied
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetAclAccess
   */
  lastAccessRequestFailedOrDenied = computed('lastAccessRequestTask.isError', 'userHasAclAccess', function(
    this: DatasetAclAccess
  ): boolean {
    const { lastAccessRequestTask, userHasAclAccess } = getProperties(this, [
      'lastAccessRequestTask',
      'userHasAclAccess'
    ]);
    const lastRequestErrored = lastAccessRequestTask && lastAccessRequestTask.isError;
    return !!lastRequestErrored && !userHasAclAccess;
  });

  /**
   * Action to reset the request form
   * @memberof DatasetAclAccess
   */
  resetForm() {
    //@ts-ignore dot notation property access
    set(this, 'userAclRequest', getDefaultRequestAccessControlEntry());
  }

  /**
   * Invokes external action when the accessType to be requested is modified
   * @param {IAccessControlAccessTypeOption} arg
   * @memberof DatasetAclAccess
   */
  @action
  onAccessTypeChange(arg: IAccessControlAccessTypeOption): void {
    get(this, 'accessTypeDidChange')(arg);
  }

  /**
   * Sets the selectedDate property on this and invokes the external action to set the expiration date
   * @param {Date} date
   * @memberof DatasetAclAccess
   */
  @action
  onExpirationDateChange(date: Date) {
    set(this, 'selectedDate', date);
    get(this, 'expiresAtDidChange')(date);
  }
}
