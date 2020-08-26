import Component from '@ember/component';
import { set } from '@ember/object';
import { action, computed } from '@ember/object';
import { filter, map, alias } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';
import { IOwner } from 'datahub-web/typings/api/datasets/owners';
import {
  ownerAlreadyExists,
  confirmOwner,
  updateOwner,
  minRequiredConfirmedOwners,
  validConfirmedOwners,
  isRequiredMinOwnersNotConfirmed,
  isConfirmedOwner
} from 'datahub-web/constants/datasets/owner';
import { OwnerSource, OwnerType } from 'datahub-web/utils/api/datasets/owners';
import Notifications from '@datahub/utils/services/notifications';
import { noop } from 'lodash';
import { makeAvatar } from 'datahub-web/constants/avatars/avatars';
import { OwnerWithAvatarRecord } from 'datahub-web/typings/app/datasets/owners';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';

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
  save: (owners: Array<IOwner>) => Promise<Array<IOwner>> = () => Promise.resolve([]);

  /**
   * The list of owners
   * @type {Array<IOwner>}
   * @memberof DatasetAuthors
   */
  owners: Array<IOwner> = [];

  /**
   * The list of suggested owners, used to populate the suggestions window below the owners table
   * @type {Array<IOwner>}
   * @memberof DatasetAuthors
   */
  suggestedOwners: Array<IOwner> = [];

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

  setOwnershipRuleChange: (notConfirmed: boolean) => void = noop;

  /**
   * Flag that resolves in the affirmative if the number of confirmed owner is less the minimum required
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetAuthors
   */
  @computed('confirmedOwners.@each.type')
  get requiredMinNotConfirmed(): boolean {
    const { changedState } = this;

    if (changedState < 1) {
      set(this, 'changedState', (changedState + 1) as Comparator);
    }
    // If there have been no changes, then we want to automatically set true in order to disable save button
    // when no changes have been made
    const requiredOwnersNotConfirmed = isRequiredMinOwnersNotConfirmed(this.confirmedOwners);
    this.setOwnershipRuleChange(requiredOwnersNotConfirmed);
    return changedState === -1 || requiredOwnersNotConfirmed;
  }

  /**
   * Counts the number of valid confirmed owners needed to make changes to the dataset
   * @type {ComputedProperty<number>}
   * @memberof DatasetAuthors
   */
  @computed('confirmedOwners.@each.type')
  get ownersRequiredCount(): number {
    return minRequiredConfirmedOwners - validConfirmedOwners(this.confirmedOwners).length;
  }

  /**
   * Lists the owners that have be confirmed view the client ui
   * @param {IOwner} owner the current owner to check if confirmed
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  @filter('owners', function(owner: IOwner): boolean {
    return isConfirmedOwner(owner);
  })
  _confirmedOwnersFilter: Array<IOwner>;

  /**
   * Aliases the _confirmedOwnersFilter. This is a workaround for
   * @link https://github.com/ember-decorators/ember-decorators/issues/407
   * @type {Array<IOwner>}
   * @memberof DatasetAuthors
   */
  @alias('_confirmedOwnersFilter')
  confirmedOwners: Array<IOwner>;

  /**
   * Augments an IOwner instance with an IAvatar Record keyed by 'avatar'
   * @this {DatasetAuthors}
   * @param owner
   * @memberof DatasetAuthors
   * @returns {OwnerWithAvatarRecord}
   */
  datasetAuthorsOwnersAugmentedWithAvatars = (owner: IOwner): OwnerWithAvatarRecord => {
    const { avatarProperties } = this;

    return {
      owner,
      avatar: avatarProperties
        ? makeAvatar(avatarProperties)({ userName: owner.userName })
        : { imageUrl: '', imageUrlFallback: '/assets/images/default_avatar.png' },
      profile: PersonEntity.profileLinkFromUsername(owner.userName)
    };
  };

  /**
   * Augments each confirmedOwner IOwner instance with an avatar Record
   * @param {IOwner} owner the IOwner instance
   * @returns {OwnerWithAvatarRecord}
   * @memberof DatasetAuthors
   */
  @map('confirmedOwners', function(this: DatasetAuthors, owner: IOwner): OwnerWithAvatarRecord {
    return this.datasetAuthorsOwnersAugmentedWithAvatars(owner);
  })
  confirmedOwnersWithAvatars: Array<OwnerWithAvatarRecord>;

  /**
   * Intersection of confirmed owners and suggested owners
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  @computed('{confirmedOwners,systemGeneratedOwners}.@each.userName')
  get commonOwners(): Array<IOwner> {
    const { confirmedOwners, systemGeneratedOwners } = this;

    return confirmedOwners.reduce((common, owner): Array<IOwner> => {
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
    return (this.suggestedOwners || []).slice(0);
  }

  /**
   * Augments each systemGeneratedOwner IOwner instance with an avatar Record
   * @param {IOwner} owner the IOwner instance
   * @returns {OwnerWithAvatarRecord}
   * @memberof DatasetAuthors
   */
  @map('systemGeneratedOwners', function(this: DatasetAuthors, owner: IOwner): OwnerWithAvatarRecord {
    return this.datasetAuthorsOwnersAugmentedWithAvatars(owner);
  })
  systemGeneratedOwnersWithAvatars: Array<OwnerWithAvatarRecord>;
  /**
   * Invokes the external action as a dropping task
   * @memberof DatasetAuthors
   */
  @(task(function*(this: DatasetAuthors): IterableIterator<Promise<Array<IOwner>>> {
    yield this.save(this.owners);
  }).drop())
  saveOwners!: ETaskPromise<Array<IOwner>>;

  /**
   * Action triggered when user clicks on add an owner
   */
  @action
  onClickAddOwner(): void {
    this.set('isAddingOwner', true);
  }

  /**
   * Adds the component owner record to the list of owners with default props
   * @returns {Array<IOwner> | void}
   */
  @action
  addOwner(this: DatasetAuthors, newOwner: IOwner): Array<IOwner> | void {
    const {
      owners = [],
      notifications: { notify }
    } = this;

    if (ownerAlreadyExists(owners, { userName: newOwner.userName, source: newOwner.source })) {
      return void notify({ content: 'Owner has already been added to "confirmed" list', type: NotificationEvent.info });
    }

    const { username = '' } = this.currentUser?.entity || {};
    const updatedOwners = [...owners, newOwner];
    confirmOwner(newOwner, username);

    return owners.setObjects(updatedOwners);
  }

  /**
   * Updates the type attribute for a given owner in the owner list
   * @param {IOwner} owner owner to be updates
   * @param {OwnerType} type new value to be set on the type attribute
   */
  @action
  updateOwnerType(this: DatasetAuthors, owner: IOwner, type: OwnerType): Array<IOwner> | void {
    const owners = this.owners || [];
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
    const owners = this.owners || [];
    return owners.removeObject(owner);
  }
}
