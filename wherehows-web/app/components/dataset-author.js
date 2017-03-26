import Ember from 'ember';

const {
  set,
  get,
  getWithDefault,
  computed,
  Component,
  inject: {service}
} = Ember;

const userNameEditableClass = 'dataset-author-cell--editing';
const minRequiredConfirmed = 2;

export default Component.extend({
  // Inject the currentUser service to retrieve logged in userName
  currentUser: service(),

  $ownerTable: null,

  // Returns a list of owners with truthy value for their confirmedBy attribute
  confirmedOwners: computed('owners.@each.confirmedBy', function () {
    return getWithDefault(this, 'owners', []).filter(
      ({confirmedBy}) => confirmedBy
    );
  }),

  // Checks that the number of confirmedOwners is < minRequiredConfirmed
  requiredMinNotConfirmed: computed.lt('confirmedOwners.length', minRequiredConfirmed),

  didInsertElement() {
    this._super(...arguments);
    // Cache reference to element on component
    this.set('$ownerTable', $('[data-attribute=owner-table]'));

    // Apply jQuery sortable plugin to element
    this.get('$ownerTable')
        .sortable({
          start: (e, {item}) => this.set('startPosition', item.index()),

          update: (e, {item}) => {
            const startPosition = this.get('startPosition');
            const endPosition = item.index(); // New position where UI element was dropped
            const owners = this.get('owners') || [];

            // Updates the owners array to reflect the UI position changes
            if (owners.length) {
              const _owners = owners.slice(0);
              const updatedOwner = _owners.splice(startPosition, 1).pop();
              _owners.splice(endPosition, 0, updatedOwner);
              owners.setObjects(_owners);
              setOwnerNameAutocomplete(this.controller);
            }
          }
        });
  },

  willDestroyElement() {
    this._super(...arguments);
    // Removes the sortable functionality from the cached DOM element reference
    get(this, '$ownerTable').sortable('destroy');
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
    editUserName(owner, {target: {value, parentNode = {}}}) {
      const {nodeName, classList} = parentNode;
      if (value) {
        set(owner, 'userName', value);
      }

      // The containing TD should have the className that allows the user
      //   to see the input element, remove this to revert to readonly mode
      if (String(nodeName).toLowerCase() === 'td') {
        classList.remove(userNameEditableClass);
      }
    },

    addOwner: function (data) {
      var owners = data;
      var controller = this.get("parentController");
      var currentUser = this.get("currentUser");
      var addedOwner = {
        "userName": "Owner", "email": null, "name": "", "isGroup": false,
        "namespace": "urn:li:griduser", "type": "Producer", "subType": null, "sortId": 0
      };
      var userEntitiesSource = controller.get("userEntitiesSource");
      var userEntitiesMaps = controller.get("userEntitiesMaps");
      var exist = false;
      if (owners && owners.length > 0) {
        owners.forEach(function (owner) {
          if (owner.userName == addedOwner.userName) {
            exist = true;
          }
        });
      }
      if (!exist) {
        owners.unshiftObject(addedOwner);
        setTimeout(function () {
          setOwnerNameAutocomplete(controller)
        }, 500);
      }
      else {
        console.log("The owner is already exist");
      }
    },
    removeOwner: function (owners, owner) {
      if (owners && owner) {
        owners.removeObject(owner);
      }
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
    confirmOwner(owner, {target: {checked} = false}) {
      // Attempt to get the userName from the currentUser service
      const userName = get(this, 'currentUser.userName') ||
        get(this, 'currentUser.user.userName');

      // If checked, assign userName otherwise, null
      const isConfirmedBy = checked ? userName : null;
      const currentOwners = get(this, 'owners');

      // Ensure that the provided owner is in the list of currentOwners
      if (currentOwners.includes(owner)) {
        const ownerPosition = currentOwners.indexOf(owner);
        // If we have a non blank userName in confirmedBy
        //  assign to owner, otherwise assign a toggled blank.
        //  A toggled blank is necessary as a state change device, since
        //  Ember does a deep comparison of object properties when deciding
        //  to perform a re-render. We could also use a device such as a hidden
        //  field with a randomized value.
        //  This helps to ensure our UI checkbox is consistent with our world
        //  state.
        const currentConfirmation = get(owner, 'confirmedBy');
        let nextBlank = currentConfirmation === null ? void 0 : null;

        set(owner, 'confirmedBy', isConfirmedBy ? isConfirmedBy : nextBlank);

        // Non-mutative array insertion
        const updatedOwners = [
          ...currentOwners.slice(0, ownerPosition),
          owner,
          ...currentOwners.slice(ownerPosition + 1)
        ];

        currentOwners.setObjects(updatedOwners);
      }
    },

    updateOwners: function (owners) {
      let controller = this.get("parentController");
      var showMsg = controller.get("showMsg");
      if (showMsg) {
        return;
      }
      var model = controller.get("model");
      if (!model || !model.id) {
        return;
      }
      var url = "/api/v1/datasets/" + model.id + "/owners";
      var token = ($("#csrfToken").val() || '').replace('/', '');
      $.ajax({
        url: url,
        method: 'POST',
        header: {
          'Csrf-Token': token
        },
        data: {
          csrfToken: token,
          owners: JSON.stringify(owners)
        }
      }).done(function (data, txt, xhr) {
        if (data.status == "success") {
          this.set('showMsg', true);
          this.set('alertType', "alert-success");
          this.set('ownerMessage', "Ownership successfully updated.");
        }
        else {
          this.set('showMsg', true);
          this.set('alertType', "alert-danger");
          this.set('ownerMessage', "Ownership update failed.");
        }
      }.bind(this)).fail(function (xhr, txt, error) {
        this.set('showMsg', true);
        this.set('alertType', "alert-danger");
        this.set('ownerMessage', "Ownership update failed.");
      });
    }
  }
});
