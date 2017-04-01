import Ember from 'ember';

const {
  set,
  get,
  getWithDefault,
  computed,
  Component,
  inject: { service }
} = Ember;

// Class to toggle readonly mode vs edit mode
const userNameEditableClass = 'dataset-author-cell--editing';
//  Required minimum confirmed owners needed to update the list of owners
const minRequiredConfirmed = 2;
const _defaultOwnerProps = {
  userName: 'New Owner',
  email: null,
  name: '',
  isGroup: false,
  namespace: 'urn:li:griduser',
  type: 'Producer',
  subType: null,
  sortId: 0,
  source: ''
};

export default Component.extend({
  // Inject the currentUser service to retrieve logged in userName
  sessionUser: service('current-user'),
  $ownerTable: null,
  disableConfirmOwners: true,

  loggedInUserName() {
    return get(this, 'sessionUser.userName') ||
      get(this, 'sessionUser.currentUser.userName');
  },

  // Returns a list of owners with truthy value for their confirmedBy attribute,
  //   i.e. they confirmedBy contains a userName and
  //   type is `Owner` and idType is `USER`.
  confirmedOwners: computed('owners.[]', function () {
    return getWithDefault(this, 'owners', []).filter(
      ({ confirmedBy, type, idType }) => confirmedBy &&
      type === 'Owner' &&
      idType === 'USER'
    );
  }),

  // Checks that the number of confirmedOwners is < minRequiredConfirmed
  requiredMinNotConfirmed: computed.lt(
    'confirmedOwners.length',
    minRequiredConfirmed
  ),

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

          // TODO: refactor in next commit
          // setOwnerNameAutocomplete(this.controller);
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
   * Non mutative update to a specific owner on the list of current owners.
   * @param {Object} owner the current props on the owner to be updated
   * @param {String} prop the property to update on the owner in the list of owners
   * @param {*} value the value to set on the owner in the source list
   * @private
   */
  _updateOwner(owner, prop, value) {
    const sourceOwners = get(this, 'owners');
    const updatingOwners = [...sourceOwners];

    // Ensure that the provided owner is in the list of sourceOwners
    if (updatingOwners.includes(owner)) {
      const ownerPosition = updatingOwners.indexOf(owner);
      const updatedOwner = Object.assign({}, owner, { [prop]: value });

      // Non-mutative array insertion
      const updatedOwners = [
        ...updatingOwners.slice(0, ownerPosition),
        updatedOwner,
        ...updatingOwners.slice(ownerPosition + 1)
      ];

      // Full reset of the `owners` list with the new list
      sourceOwners.setObjects(updatedOwners);
    }
  },

  actions: {
    /**
     * Handles the user intention to update the owner userName, by
     *   adding a class signifying edit to the DOM td classList.
     *   The click event is handled on the TD which is the parent
     *   of the target label.
     * @param {DOMTokenList} classList passed in from the HTMLBars template
     *   currentTarget.classList
     */
    willEditUserName(classList) {
      // Add the className so the input element is visible in the DOM
      classList.add(userNameEditableClass);
    },

    /**
     * Mutates the owner.userName property, setting it to the value entered
     *   in the table field
     * @param {Object} owner the owner object rendered for this table row
     * @param {String|void} value user entered value from the ui
     * @param {Node} parentNode DOM node wrapping the target user input field
     *   this is expected to be a TD node
     */
    editUserName(owner, { target: { value, parentNode = {} } }) {
      const { nodeName, classList } = parentNode;
      if (value) {
        set(owner, 'userName', value);
      }

      // The containing TD should have the className that allows the user
      //   to see the input element, remove this to revert to readonly mode
      if (String(nodeName).toLowerCase() === 'td') {
        classList.remove(userNameEditableClass);
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

      const ownerAlreadyExists = currentOwners
        .mapBy('userName')
        .some(userName => regex.test(userName));

      if (!ownerAlreadyExists) {
        updatedOwners = [newOwner, ...currentOwners];
        return set(this, 'owners', updatedOwners);
      }

      set(
        this,
        'errorMessage',
        `Uh oh! There is already a user with the username ${newOwner.userName} in the list of owners.`
      );

      // TODO: refactor in next commit
      setTimeout(
        function() {
          // setOwnerNameAutocomplete(controller)
        },
        500
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
    confirmOwner(owner, { target: { checked } = false }) {
      // Attempt to get the userName from the currentUser service
      const userName = get(this, 'loggedInUserName').call(this);
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

      return typeof closureAction === 'function' &&
        closureAction(get(this, 'owners'))
          .then(({
            status
          } = {}) => {
            if (status === 'ok') {
              set(
                this,
                'actionMessage',
                'Successfully updated ownership for this dataset'
              );
            }
          })
          .catch(({ msg }) =>
            set(
              this,
              'errorMessage',
              `An error occurred while saving. Please let us know of this incident. ${msg}`
            ));
    }
  }
});
