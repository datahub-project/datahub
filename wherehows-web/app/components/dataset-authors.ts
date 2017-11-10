import Component from '@ember/component';
import { inject } from '@ember/service';
import ComputedProperty, { or, lt, filter } from '@ember/object/computed';
import { set, get, computed, getProperties } from '@ember/object';
import { assert } from '@ember/debug';
import { isEqual } from 'lodash';

import UserLookup from 'wherehows-web/services/user-lookup';
import CurrentUser from 'wherehows-web/services/current-user';
import { IOwner } from 'wherehows-web/typings/api/datasets/owners';
import {
  defaultOwnerProps,
  defaultOwnerUserName,
  minRequiredConfirmedOwners,
  ownerAlreadyExists,
  userNameEditableClass,
  confirmOwner,
  updateOwner
} from 'wherehows-web/constants/datasets/owner';
import { OwnerSource, OwnerIdType, OwnerType } from 'wherehows-web/utils/api/datasets/owners';
import { ApiStatus } from 'wherehows-web/utils/api';

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
   * @return {Promise<{ status: ApiStatus }>}
   * @memberof DatasetAuthors
   */
  save: (owners: Array<IOwner>) => Promise<{ status: ApiStatus }>;

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
   * User look up service
   * @type {ComputedProperty<UserLookup>}
   * @memberof DatasetAuthors
   */
  userLookup: ComputedProperty<UserLookup> = inject();

  /**
   * Reference to the userNamesResolver function to asynchronously match userNames
   * @type {UserLookup['userNamesResolver']}
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
   * Computed flag indicating that a set of negative flags is true
   * e.g. if the userName is invalid or the required minimum users are not confirmed
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetAuthors
   */
  ownershipIsInvalid: ComputedProperty<boolean> = or('userNameInvalid', 'requiredMinNotConfirmed');

  /**
   * Checks that the list of owners does not contain a default user name
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetAuthors
   */
  userNameInvalid: ComputedProperty<boolean> = computed('owners.[]', function(this: DatasetAuthors) {
    const owners = get(this, 'owners') || [];

    return owners.filter(({ userName }) => userName === defaultOwnerUserName).length > 0;
  });

  /**
   * Flag that resolves in the affirmative if the number of confirmed owner is less the minimum required
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetAuthors
   */
  requiredMinNotConfirmed: ComputedProperty<boolean> = lt('confirmedOwners.length', minRequiredConfirmedOwners);

  /**
   * Lists the owners that have be confirmed view the client ui
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  confirmedOwners: ComputedProperty<Array<IOwner>> = filter('owners', function({ source }: IOwner) {
    return source === OwnerSource.Ui;
  });

  /**
   * Intersection of confirmed owners and suggested owners
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  commonOwners: ComputedProperty<Array<IOwner>> = computed(
    'confirmedOwners.@each.userName',
    'systemGeneratedOwners.@each.userName',
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
   * Lists owners that have been gleaned from dataset metadata
   * @type {ComputedProperty<Array<IOwner>>}
   * @memberof DatasetAuthors
   */
  systemGeneratedOwners: ComputedProperty<Array<IOwner>> = filter('owners', function({ source }: IOwner) {
    return source !== OwnerSource.Ui;
  });

  constructor() {
    super(...arguments);
    const typeOfSaveAction = typeof this.save;

    // on instantiation, sets a reference to the userNamesResolver async function
    set(this, 'userNamesResolver', get(get(this, 'userLookup'), 'userNamesResolver'));

    assert(
      `Expected action save to be an function (Ember action), got ${typeOfSaveAction}`,
      typeOfSaveAction === 'function'
    );
  }

  actions = {
    /**
     * Prepares component for updates to the userName attribute
     * @param {IOwner} _ unused
     * @param {HTMLElement} { currentTarget }
     */
    willEditUserName(_: IOwner, { currentTarget }: Event) {
      const { classList } = <HTMLElement>(currentTarget || {});
      if (classList instanceof HTMLElement) {
        classList.add(userNameEditableClass);
      }
    },

    /**
     * Updates the owner instance userName property
     * @param {IOwner} currentOwner an instance of an IOwner type to be updates
     * @param {string} [userName] optional userName to update to
     */
    editUserName: async (currentOwner: IOwner, userName?: string) => {
      if (userName) {
        const { getPartyEntityWithUserName } = get(this, 'userLookup');
        const partyEntity = await getPartyEntityWithUserName(userName);

        if (partyEntity) {
          const { label, displayName, category } = partyEntity;
          const isGroup = category === 'group';
          const updatedOwnerProps: IOwner = {
            ...currentOwner,
            isGroup,
            source: OwnerSource.Ui,
            userName: label,
            name: displayName,
            idType: isGroup ? OwnerIdType.Group : OwnerIdType.User
          };

          updateOwner(get(this, 'owners'), currentOwner, updatedOwnerProps);
        }
      }
    },

    /**
     * Adds the component owner record to the list of owners with default props
     * @returns {Array<IOwner> | void}
     */
    addOwner: () => {
      const owners = get(this, 'owners') || [];
      const newOwner: IOwner = { ...defaultOwnerProps };

      if (!ownerAlreadyExists(owners, { userName: newOwner.userName })) {
        const { userName } = get(get(this, 'currentUser'), 'currentUser');
        let updatedOwners = [newOwner, ...owners];
        confirmOwner(get(this, 'owners'), newOwner, userName);

        return owners.setObjects(updatedOwners);
      }
    },

    /**
     * Updates the type attribute for a given owner in the owner list
     * @param {IOwner} owner owner to be updates
     * @param {OwnerType} type new value to be set on the type attribute
     */
    updateOwnerType: (owner: IOwner, type: OwnerType) => {
      const owners = get(this, 'owners') || [];
      return updateOwner(owners, owner, 'type', type);
    },

    /**
     * Adds the owner instance to the list of owners with the source set to ui
     * @param {IOwner} owner the owner to add to the list of owner with the source set to OwnerSource.Ui
     * @return {Array<IOwner> | void}
     */
    confirmSuggestedOwner: (owner: IOwner) => {
      const owners = get(this, 'owners') || [];
      const suggestedOwner = { ...owner, source: OwnerSource.Ui };
      const hasSuggested = owners.find(owner => isEqual(owner, suggestedOwner));

      if (!hasSuggested) {
        return owners.setObjects([...owners, suggestedOwner]);
      }
    },

    /**
     * removes an owner instance from the list of owners
     * @param {IOwner} owner the owner to be removed
     */
    removeOwner: (owner: IOwner) => {
      const owners = get(this, 'owners') || [];
      return owners.removeObject(owner);
    },

    /**
     * Persists the owners list by invoking the external action
     */
    saveOwners: () => {
      const { save } = this;
      save(get(this, 'owners'));
    }
  };
}
