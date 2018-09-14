import Component from '@ember/component';
import { set, get, getProperties } from '@ember/object';
import { assert } from '@ember/debug';
import { action, computed } from '@ember-decorators/object';
import { filter, map } from '@ember-decorators/object/computed';
import { service } from '@ember-decorators/service';
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
  isRequiredMinOwnersNotConfirmed,
  isConfirmedOwner
} from 'wherehows-web/constants/datasets/owner';
import { OwnerSource, OwnerType } from 'wherehows-web/utils/api/datasets/owners';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { noop } from 'wherehows-web/utils/helpers/functions';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { getAvatarProps } from 'wherehows-web/constants/avatars/avatars';

type Comparator = -1 | 0 | 1;

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
   * The list of suggested owners, used to populate the suggestions window below the owners table
   * @type {Array<IOwner>}
   * @memberof DatasetAuthors
   */
  suggestedOwners: Array<IOwner>;

  /**
   * Avatar properties used to generate avatar images
   * @type {(IAppConfig['userEntityProps'] | undefined)}
   * @memberof DatasetAuthors
   */
  avatarProperties: IAppConfig['userEntityProps'] | undefined;

  /**
   * Current user service
   * @type {ComputedProperty<CurrentUser>}
   * @memberof DatasetAuthors
   */
  @service('current-user')
  currentUser: CurrentUser;

  /**
   * Application notifications service
   * @type {ComputedProperty<Notifications>}
   * @memberof DatasetAuthors
   */
  @service
  notifications: Notifications;

  /**
   * User look up service
   * @type {ComputedProperty<UserLookup>}
   * @memberof DatasetAuthors
   */
  @service('user-lookup')
  userLookup: UserLookup;

  /**
   * If there are no changes to the ownership tab, we want to keep the save button disabled. Rather than
   * try to compare two sets of prev vs new data, we just have a flag here that short stops the validation
   * function.
   * Note: changedState has 3 states:
   * -1   Component hasn't completed initialization yet, and no changes have been made. When
   *      requiredMinNotConfirmed is run on init/render, this gets incremented to its neutral state at 0
   * 0    No changes have been made yet
   * 1    At least one change has been made
   * @type {number}
   * @memberof DatasetAuthors
   */
  changedState: Comparator = -1;

  /**
   * Reference to the userNamesResolver function to asynchronously match userNames
   * @type {UserLookup.userNamesResolver}
   * @memberof DatasetAuthors
   */
  userNamesResolver: UserLookup['userNamesResolver'];

  /**
   * A list of valid owner type strings returned from the remote api endpoint
   * @type {Array<OwnerType>}
   * @memberof DatasetAuthors
   */
  ownerTypes: Array<OwnerType>;

  /**
   * Boolean that determines whether the user is currently using the typeahead box to add an
   * owner. This is triggered to true when the user clicks on Add an Owner in the table and
   * returns to false at the end of the addOwner action
   * @type {boolean}
   * @default false
   * @memberof DatasetAuthors
   */
  isAddingOwner = false;

  setOwnershipRuleChange: (notConfirmed: boolean) => void;

  /**
   * Flag that resolves in the affirmative if the number of confirmed owner is less the minimum required
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetAuthors
   */
  @computed('confirmedOwners.@each.type')
  get requiredMinNotConfirmed(): boolean {
    const changedState = get(this, 'changedState');

    if (changedState < 1) {
      set(this, 'changedState', <Comparator>(changedState + 1));
    }
    // If there have been no changes, then we want to automatically set true in order to disable save button
    // when no changes have been made
    const requiredOwnersNotConfirmed = isRequiredMinOwnersNotConfirmed(get(this, 'confirmedOwners'));
    get(this, 'setOwnershipRuleChange')(requiredOwnersNotConfirmed);
    return changedState === -1 || requiredOwnersNotConfirmed;
  }

  /**
   * Counts the number of valid confirmed owners needed to make changes to the dataset
   * @type {ComputedProperty<number>}
   * @memberof DatasetAuthors
   */
  @computed('confirmedOwners.@each.type')
  get ownersRequiredCount(): number {
    return minRequiredConfirmedOwners - validConfirmedOwners(get(this, 'confirmedOwners')).length;
  }

  /**
   * Lists the owners that have be confirmed view the client ui
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  @filter('owners', isConfirmedOwner)
  confirmedOwners: Array<IOwner>;

  /**
   * Augments each confirmedOwner IOwner instance with an avatar Record
   * @param {DatasetAuthors} this
   * @param {IOwner} owner the IOwner instance
   * @returns {(IOwner & Record<'avatar', IAvatar>)}
   * @memberof DatasetAuthors
   */
  @map('confirmedOwners')
  confirmedOwnersWithAvatars(this: DatasetAuthors, owner: IOwner): IOwner & Record<'avatar', IAvatar> {
    const { avatarProperties } = this;

    return {
      ...owner,
      avatar: avatarProperties
        ? getAvatarProps(avatarProperties)({ userName: owner.userName })
        : { imageUrl: '', imageUrlFallback: '/assets/assets/images/default_avatar.png' }
    };
  }

  /**
   * Intersection of confirmed owners and suggested owners
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  @computed('{confirmedOwners,systemGeneratedOwners}.@each.userName')
  get commonOwners(): Array<IOwner> {
    const { confirmedOwners = [], systemGeneratedOwners = [] } = getProperties(this, [
      'confirmedOwners',
      'systemGeneratedOwners'
    ]);

    return confirmedOwners.reduce((common, owner) => {
      const { userName } = owner;
      return systemGeneratedOwners.findBy('userName', userName) ? [...common, owner] : common;
    }, []);
  }

  /**
   * Lists owners that have been gleaned from dataset metadata,
   * filters out owners that have a source that is NOT OwnerSource.Ui and idType that IS OwnerIdType.User
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  @computed('suggestedOwners')
  get systemGeneratedOwners(): Array<IOwner> {
    // Creates a copy of suggested owners since using it directly seems to invoke a "modified twice in the
    // same render" error
    return (get(this, 'suggestedOwners') || []).slice(0);
  }

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

    this.setOwnershipRuleChange || (this.setOwnershipRuleChange = noop);
  }

  /**
   * Adds the component owner record to the list of owners with default props
   * @returns {Array<IOwner> | void}
   */
  @action
  addOwner(this: DatasetAuthors, newOwner: IOwner): Array<IOwner> | void {
    const owners = get(this, 'owners') || [];
    const { notify } = get(this, 'notifications');

    if (ownerAlreadyExists(owners, { userName: newOwner.userName, source: newOwner.source })) {
      return void notify(NotificationEvent.info, { content: 'Owner has already been added to "confirmed" list' });
    }

    const { userName } = get(get(this, 'currentUser'), 'currentUser');
    const updatedOwners = [...owners, newOwner];
    confirmOwner(newOwner, userName);

    return owners.setObjects(updatedOwners);
  }

  /**
   * Updates the type attribute for a given owner in the owner list
   * @param {IOwner} owner owner to be updates
   * @param {OwnerType} type new value to be set on the type attribute
   */
  @action
  updateOwnerType(this: DatasetAuthors, owner: IOwner, type: OwnerType): Array<IOwner> | void {
    const owners = get(this, 'owners') || [];
    return updateOwner(owners, owner, 'type', type);
  }

  /**
   * Adds the owner instance to the list of owners with the source set to ui
   * @param {IOwner} owner the owner to add to the list of owner with the source set to OwnerSource.Ui
   * @return {Array<IOwner> | void}
   */
  @action
  confirmSuggestedOwner(this: DatasetAuthors, owner: IOwner): Array<IOwner> | void {
    const suggestedOwner = { ...owner, source: OwnerSource.Ui };
    return this.actions.addOwner.call(this, suggestedOwner);
  }

  /**
   * removes an owner instance from the list of owners
   * @param {IOwner} owner the owner to be removed
   */
  @action
  removeOwner(this: DatasetAuthors, owner: IOwner): IOwner {
    const owners = get(this, 'owners') || [];
    return owners.removeObject(owner);
  }
}
