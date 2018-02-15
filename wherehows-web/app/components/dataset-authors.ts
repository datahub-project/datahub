import Component from '@ember/component';
import { inject } from '@ember/service';
import ComputedProperty, { filter } from '@ember/object/computed';
import { set, get, computed, getProperties } from '@ember/object';
import { assert } from '@ember/debug';
import { task } from 'ember-concurrency';

import UserLookup from 'wherehows-web/services/user-lookup';
import CurrentUser from 'wherehows-web/services/current-user';
import { IOwner } from 'wherehows-web/typings/api/datasets/owners';
import {
  ownerAlreadyExists,
  confirmOwner,
  updateOwner,
  minRequiredConfirmedOwners,
  validConfirmedOwners,
  isRequiredMinOwnersNotConfirmed
} from 'wherehows-web/constants/datasets/owner';
import { OwnerIdType, OwnerSource, OwnerType } from 'wherehows-web/utils/api/datasets/owners';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';

/**
 * Defines properties for the component that renders a list of owners and provides functionality for
 * interacting with the list items or the list as whole
 * @export
 * @class DatasetAuthors
 * @extends {Component}
 */
export default class DatasetAuthors extends Component {
  /**
   * Invokes an external save action to persist the list of owners
   * @return {Promise<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  save: (owners: Array<IOwner>) => Promise<Array<IOwner>>;

  /**
   * The list of owners
   * @type {Array<IOwner>}
   * @memberof DatasetAuthors
   */
  owners: Array<IOwner>;

  /**
   * Current user service
   * @type {ComputedProperty<CurrentUser>}
   * @memberof DatasetAuthors
   */
  currentUser: ComputedProperty<CurrentUser> = inject();

  /**
   * Application notifications service
   * @type {ComputedProperty<Notifications>}
   * @memberof DatasetAuthors
   */
  notifications: ComputedProperty<Notifications> = inject();

  /**
   * User look up service
   * @type {ComputedProperty<UserLookup>}
   * @memberof DatasetAuthors
   */
  userLookup: ComputedProperty<UserLookup> = inject();

  /**
   * Reference to the userNamesResolver function to asynchronously match userNames
   * @type {UserLookup.userNamesResolver}
   * @memberof DatasetAuthors
   */
  userNamesResolver: UserLookup['userNamesResolver'];

  /**
   * A list of valid owner type strings returned from the remote api endpoint
   * @type {Array<string>}
   * @memberof DatasetAuthors
   */
  ownerTypes: Array<string>;

  /**
   * Flag that resolves in the affirmative if the number of confirmed owner is less the minimum required
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetAuthors
   */
  requiredMinNotConfirmed: ComputedProperty<boolean> = computed('confirmedOwners.length', function(
    this: DatasetAuthors
  ) {
    return isRequiredMinOwnersNotConfirmed(get(this, 'confirmedOwners'));
  });

  /**
   * Counts the number of valid confirmed owners needed to make changes to the dataset
   * @type {ComputedProperty<number>}
   * @memberof DatasetAuthors
   */
  ownersRequiredCount: ComputedProperty<number> = computed('confirmedOwners.[]', function(this: DatasetAuthors) {
    return minRequiredConfirmedOwners - validConfirmedOwners(get(this, 'confirmedOwners')).length;
  });

  /**
   * Lists the owners that have be confirmed view the client ui
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  confirmedOwners: ComputedProperty<Array<IOwner>> = filter('owners', ({ source }) => source === OwnerSource.Ui);

  /**
   * Intersection of confirmed owners and suggested owners
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  commonOwners: ComputedProperty<Array<IOwner>> = computed(
    '{confirmedOwners,systemGeneratedOwners}.@each.userName',
    function(this: DatasetAuthors) {
      const { confirmedOwners = [], systemGeneratedOwners = [] } = getProperties(this, [
        'confirmedOwners',
        'systemGeneratedOwners'
      ]);

      return confirmedOwners.reduce((common, owner) => {
        const { userName } = owner;
        return systemGeneratedOwners.findBy('userName', userName) ? [...common, owner] : common;
      }, []);
    }
  );

  /**
   * Lists owners that have been gleaned from dataset metadata,
   * filters out owners that have a source that is NOT OwnerSource.Ui and idType that IS OwnerIdType.User
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  systemGeneratedOwners: ComputedProperty<Array<IOwner>> = filter('owners', function({ source, idType }: IOwner) {
    return source !== OwnerSource.Ui && idType === OwnerIdType.User;
  });

  /**
   * Invokes the external action as a dropping task
   * @type {Task<Promise<Array<IOwner>>, void>}
   * @memberof DatasetAuthors
   */
  saveOwners = task(function*(this: DatasetAuthors): IterableIterator<Promise<Array<IOwner>>> {
    yield get(this, 'save')(get(this, 'owners'));
  }).drop();

  constructor() {
    super(...arguments);
    const typeOfSaveAction = typeof this.save;

    // on instantiation, sets a reference to the userNamesResolver async function
    // @ts-ignore ts limitation with the ember object model, fixed in ember 3.1 with es5 getters
    set(this, 'userNamesResolver', get(get(this, 'userLookup'), 'userNamesResolver'));

    assert(
      `Expected action save to be an function (Ember action), got ${typeOfSaveAction}`,
      typeOfSaveAction === 'function'
    );
  }

  actions = {
    /**
     * Adds the component owner record to the list of owners with default props
     * @returns {Array<IOwner> | void}
     */
    addOwner(this: DatasetAuthors, newOwner: IOwner): Array<IOwner> | void {
      const owners = get(this, 'owners') || [];
      const { notify } = get(this, 'notifications');

      if (ownerAlreadyExists(owners, { userName: newOwner.userName, source: newOwner.source })) {
        return void notify(NotificationEvent.info, { content: 'Owner has already been added to "confirmed" list' });
      }

      const { userName } = get(get(this, 'currentUser'), 'currentUser');
      const updatedOwners = [newOwner, ...owners];
      confirmOwner(newOwner, userName);

      return owners.setObjects(updatedOwners);
    },

    /**
     * Updates the type attribute for a given owner in the owner list
     * @param {IOwner} owner owner to be updates
     * @param {OwnerType} type new value to be set on the type attribute
     */
    updateOwnerType(this: DatasetAuthors, owner: IOwner, type: OwnerType): Array<IOwner> | void {
      const owners = get(this, 'owners') || [];
      return updateOwner(owners, owner, 'type', type);
    },

    /**
     * Adds the owner instance to the list of owners with the source set to ui
     * @param {IOwner} owner the owner to add to the list of owner with the source set to OwnerSource.Ui
     * @return {Array<IOwner> | void}
     */
    confirmSuggestedOwner(this: DatasetAuthors, owner: IOwner): Array<IOwner> | void {
      const suggestedOwner = { ...owner, source: OwnerSource.Ui };
      return this.actions.addOwner.call(this, suggestedOwner);
    },

    /**
     * removes an owner instance from the list of owners
     * @param {IOwner} owner the owner to be removed
     */
    removeOwner(this: DatasetAuthors, owner: IOwner): IOwner {
      const owners = get(this, 'owners') || [];
      return owners.removeObject(owner);
    }
  };
}
