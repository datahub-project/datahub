import Ember from 'ember';

const { set, get, getWithDefault, computed, Component, inject: { service } } = Ember;

/**
 * Array of source names to restrict from user updates
 * @type {[String]}
 */
const restrictedSources = ['SCM', 'NUAGE'];
/**
 * Pattern to look for in source strings
 * Will case-insensitive find strings containing `scm` or `nuage`
 * @type {RegExp}
 */
const restrictedSourcesPattern = new RegExp(`.*${restrictedSources.join('|')}.*`, 'i');
const removeFromSourceMessage = `Owners sourced from ${restrictedSources.join(
  ', '
)} should be removed directly from that source`;

// Class to toggle readonly mode vs edit mode
const userNameEditableClass = 'dataset-author-cell--editing';
//  Required minimum confirmed owners needed to update the list of owners
const minRequiredConfirmed = 2;
/**
 * Initial user name for candidate owners
 * @type {string}
 * @private
 */
const _defaultOwnerUserName = 'New Owner';

const _defaultOwnerProps = {
  userName: _defaultOwnerUserName,
  email: null,
  name: '',
  isGroup: false,
  namespace: 'urn:li:corpGroup',
  type: 'Owner',
  subType: null,
  sortId: 0,
  source: ''
};

export default Component.extend({
  /**
   * Inject the currentUser service to retrieve logged in userName
   * @type {Ember.Service}
   */
  sessionUser: service('current-user'),

  /**
   * Lookup for userEntity objects
   * @type {Ember.Service}
   */
  ldapUsers: service('user-lookup'),

  $ownerTable: null,

  restrictedSources,

  removeFromSourceMessage,

  init() {
    this._super(...Array.from(arguments));

    // Sets a reference to the userNamesResolver function on instantiation.
    //  Typeahead component uses this function to resolve matches for user
    //  input
    set(this, 'userNamesResolver', get(this, 'ldapUsers.userNamesResolver'));
  },

  // Combination macro to check that the entered username is valid
  //  i.e. not _defaultOwnerUserName and the requiredMinConfirmed
  ownershipIsInvalid: computed.or('userNameInvalid', 'requiredMinNotConfirmed'),

  // Returns a list of owners with truthy value for their confirmedBy attribute,
  //   i.e. they confirmedBy contains a userName and
  //   type is `Owner` and idType is `USER`.
  confirmedOwners: computed('owners.[]', function() {
    return getWithDefault(this, 'owners', []).filter(
      ({ confirmedBy, type, idType }) => confirmedBy && type === 'Owner' && idType === 'USER'
    );
  }),

  /**
   * Checks that the number of confirmedOwners is < minRequiredConfirmed
   * @type {Ember.ComputedProperty}
   * @requires minRequiredConfirmed
   */
  requiredMinNotConfirmed: computed.lt('confirmedOwners.length', minRequiredConfirmed),

  /**
   * Checks that the list of owners does not contain an owner
   *   with the default userName `_defaultOwnerUserName`
   * @type {Ember.ComputedProperty}
   * @requires _defaultOwnerUserName
   */
  userNameInvalid: computed('owners.[]', function() {
    return getWithDefault(this, 'owners', []).filter(({ userName }) => userName === _defaultOwnerUserName).length > 0;
  }),

  didInsertElement() {
    this._super(...arguments);
    // Cache reference to element on component
    this.set('$ownerTable', $('[data-attribute=owner-table]'));

    // Apply jQuery sortable plugin to element
    this.get('$ownerTable').sortable({
      start: (e, { item }) => set(this, 'startPosition', item.index()),

      update: (e, { item }) => {
        const from = get(this, 'startPosition');
        const to = item.index(); // New position where row was dropped
        const currentOwners = getWithDefault(this, 'owners', []);

        // Updates the owners array to reflect the UI position changes
        if (currentOwners.length) {
          const reArrangedOwners = [...currentOwners];
          const travelingOwner = reArrangedOwners.splice(from, 1).pop();

          reArrangedOwners.splice(to, 0, travelingOwner);
          currentOwners.setObjects(reArrangedOwners);
        }
      }
    });
  },

  willDestroyElement() {
    this._super(...arguments);
    // Removes the sortable functionality from the cached DOM element reference
    get(this, '$ownerTable').sortable('destroy');
  },

  /**
   * Non mutative update to an owner on the list of current owners.
   * @example _updateOwner(currentOwner, isConfirmed, false);
   * @example _updateOwner(currentOwner, {isConfirmed: false, isGroup: true});
   *
   * @param {Object} currentProps the current props on the currentProps
   *   to be updated
   * @param {...string|object|*} args
   * @prop {String|Object} args[0] the property to update on the currentProps in
   *   the list of owners. Props can be also be an Object containing the
   *   properties mapped to updated values.
   * @prop {*} [args[1]] value to set on the currentProps in
   *   the source list, required is props is map of key -> value pairs
   * @private
   */
  _updateOwner(currentProps, ...args) {
    const [props, value] = args;
    const sourceOwners = get(this, 'owners');
    // Create a copy so in-flight mutations are not propagates to the ui
    const updatingOwners = [...sourceOwners];

    // Ensure that the provided currentProps is in the list of sourceOwners
    if (updatingOwners.includes(currentProps)) {
      const ownerPosition = updatingOwners.indexOf(currentProps);
      let updatedOwner = props;

      if (typeof props === 'string') {
        updatedOwner = Object.assign({}, currentProps, { [props]: value });
      }

      // Non-mutative array insertion
      const updatedOwners = [
        ...updatingOwners.slice(0, ownerPosition),
        updatedOwner,
        ...updatingOwners.slice(ownerPosition + 1)
      ];
      // The list of ldap userNames with sources currently in the list
      const userKeys = updatedOwners.map(({ userName, source }) => `${userName}:${source}`);
      // Checks that the userNames are not already in the list of current owners
      const hasDuplicates = new Set(userKeys).size !== userKeys.length;

      if (hasDuplicates) {
        set(
          this,
          'errorMessage',
          'Uh oh! Looks like there are duplicates in the list of owners, please remove them first.'
        );

        return;
      }

      // Full reset of the `owners` list with the new list
      sourceOwners.setObjects(updatedOwners);
    }
  },

  actions: {
    /**
     * Handles the user intention to update the owner userName, by
     *   adding a class signifying edit to the DOM td classList.
     *   Only `owners` in this list who are not sourced from `restrictedSources` (see var above) should
     *   the user be allowed to update / edit.
     *   The click event is handled on the TD which is the parent
     *   of the target label.
     * @param {String} source source of the currently interactive owner from
     *   the list
     * @param {DOMTokenList} classList retrieve the classList from the
     *   wrapping TD element, which handles the bubbled Mouse click on the
     *   label
     */
    willEditUserName({ source = '' }, { currentTarget: { classList } }) {
      // Add the className cached in `userNameEditableClass`. This renders
      //   the input element in the DOM, and removes the label from layout
      const disallowEdit = String(source).match(restrictedSourcesPattern);

      if (!disallowEdit && typeof classList.add === 'function') {
        classList.add(userNameEditableClass);
      }
    },

    /**
     *  Updates the owner.userName property, setting it to the value entered
     *   in the table input field
     * @param {Object} currentOwner the currentOwner object rendered
     *   for this table row
     * @param {String} userName the userName returned from the typeahead
     * @return {Promise.<void>}
     */
    async editUserName(currentOwner, userName) {
      set(this, 'errorMessage', '');
      if (userName) {
        // getUser returns a promise, treat as such
        const getUser = get(this, 'ldapUsers.getPartyEntityWithUserName');
        const { label, displayName, category } = await getUser(userName);
        const isGroup = category === 'group';

        const updatedProps = Object.assign({}, currentOwner, {
          isGroup,
          source: 'UI',
          userName: label,
          name: displayName,
          idType: isGroup ? 'GROUP' : 'USER'
        });

        this._updateOwner(currentOwner, updatedProps);
      }
    },

    /**
     * Adds a candidate owner to the list of updated owners to be potentially
     *   propagated to the server.
     *   If the owner already exists in the list of currentOwners, this is a no-op.
     *   Also, sets the errorMessage to reflect this occurrence.
     * @return {Array.<Object>|void}
     */
    addOwner() {
      // Make a copy of the list of current owners
      const currentOwners = [...getWithDefault(this, 'owners', [])];
      // Make a shallow copy for the new user. A shallow copy is fine,
      //  since the properties are scalars.
      const newOwner = Object.assign({}, _defaultOwnerProps);
      // Case insensitive regular exp to check that the username is not in
      //  the current list of owners
      const regex = new RegExp(`.*${newOwner.userName}.*`, 'i');
      let updatedOwners = [];

      const ownerAlreadyExists = currentOwners.mapBy('userName').some(userName => regex.test(userName));

      if (!ownerAlreadyExists) {
        updatedOwners = [newOwner, ...currentOwners];
        const owners = set(this, 'owners', updatedOwners);
        // By default, newly added Owners should be confirmed by the user adding them
        this.actions.confirmOwner.call(this, newOwner, { target: { checked: true } });

        return owners;
      }

      set(
        this,
        'errorMessage',
        `Uh oh! There is already a user with the username ${newOwner.userName} in the list of owners.`
      );
    },

    /**
     * Removes the provided owner from the current owners array. Pretty simple.
     * @param {Object} owner
     * @returns {any|Ember.Array|{}}
     */
    removeOwner(owner) {
      return getWithDefault(this, 'owners', []).removeObject(owner);
    },

    /**
     * Updates the ownerType prop in the list of owners
     * @param {Object} owner props to be updated
     * @param {String} value current owner value
     */
    updateOwnerType(owner, { target: { value } }) {
      this._updateOwner(owner, 'ownerType', value);
    },

    /**
     * Sets the checked confirmedBy property on an owner, when the user
     *   indicates this thought the UI, and if their username is
     *   available.
     * @param {Object} owner the owner object to update
     * @prop {String|null|void} owner.confirmedBy flag indicating userName
     *   that confirmed ownership, or null or void otherwise
     * @param {Boolean = false} checked flag for the current ui checked state
     */
    confirmOwner(owner, { target: { checked = false } = {} }) {
      // get the userName from the currentUser service
      // the currentUser object should exist if a session is active
      const userName = get(this, 'sessionUser.currentUser.userName');
      // If checked, assign userName otherwise, null
      const isConfirmedBy = checked ? userName : null;

      // If we have a non blank userName in confirmedBy
      //  assign to owner, otherwise assign a toggled blank.
      //  Blanking will prevent the UI updating it's state, since this will
      //  reset to value to a falsey value.

      //  A toggled blank is necessary as a state change device, since
      //  Ember does a deep comparison of object properties when deciding
      //  to perform a re-render. We could also use a device such as a hidden
      //  field with a randomized value.
      //  This helps to ensure our UI checkbox is consistent with our world
      //  state.
      const currentConfirmation = get(owner, 'confirmedBy');
      const nextBlank = currentConfirmation === null ? void 0 : null;
      const confirmedBy = isConfirmedBy ? isConfirmedBy : nextBlank;

      this._updateOwner(owner, 'confirmedBy', confirmedBy);
    },

    /**
     * Invokes the passed in `save` closure action with the current list of owners.
     * @returns {boolean|Promise.<TResult>|*}
     */
    updateOwners() {
      const closureAction = get(this, 'attrs.save');

      return (
        typeof closureAction === 'function' &&
        closureAction(get(this, 'owners'))
          .then(({ status } = {}) => {
            if (status === 'ok') {
              set(this, 'actionMessage', 'Successfully updated ownership for this dataset');
            }
          })
          .catch(({ msg }) =>
            set(this, 'errorMessage', `An error occurred while saving. Please let us know of this incident. ${msg}`)
          )
      );
    }
  }
});
