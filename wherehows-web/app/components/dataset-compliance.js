import Ember from 'ember';

export default Ember.Component.extend({
  searchTerm: '',
  complianceType: Ember.computed.alias('privacyCompliancePolicy.complianceType'),
  get complianceTypes() {
    return ['CUSTOM_PURGE', 'AUTO_PURGE', 'RETENTION_PURGE', 'NOT_APPLICABLE'].map(complianceType => ({
      value: complianceType,
      label: complianceType.replace('_', ' ').toLowerCase().capitalize()
    }))
  },

  // Cached list of dataset field names
  datasetSchemaFieldNames: Ember.computed('datasetSchemaFieldsAndTypes', function () {
    return this.get('datasetSchemaFieldsAndTypes').mapBy('name');
  }),

  matchingFields: Ember.computed('searchTerm', 'datasetSchemaFieldsAndTypes', function () {
    if (this.get('datasetSchemaFieldsAndTypes')) {
      const searchTerm = this.get('searchTerm');
      const matches = $.ui.autocomplete.filter(this.get('datasetSchemaFieldNames'), searchTerm);
      return matches.map(value => {
        const {type} = this.get('datasetSchemaFieldsAndTypes').filterBy('name', value).get('firstObject');
        const dataType = Array.isArray(type) && type.toString().toUpperCase();

        return {
          value,
          dataType
        };
      });
    }
  }),

  /**
   * Aliases compliancePurgeEntities on privacyCompliancePolicy, and transforms each nested comma-delimited identifierField string
   * into an array of fields that can easily be iterated over. Dependency on each identifierField will update
   * UI on updates
   */
  purgeEntities: Ember.computed('privacyCompliancePolicy.compliancePurgeEntities.@each.identifierField', function () {
    const compliancePurgeEntities = this.get('privacyCompliancePolicy.compliancePurgeEntities');
    // Type ENUM is locally saved in the client.
    // The user is able to add a values to the identifier types that are not provided in the schema returned by the server.
    // Values are manually extracted from Nuage schema at develop time
    const purgeableEntityFieldIdentifierTypes = [
      'MEMBER_ID', 'SUBJECT_MEMBER_ID', 'URN', 'SUBJECT_URN', 'COMPANY_ID', 'GROUP_ID', 'CUSTOMER_ID'
    ];

    // Create an object list for each purgeableEntityFieldIdentifier with a mapping to label and identifierField value
    return purgeableEntityFieldIdentifierTypes.map(identifierType => {
      // Find entity with matching identifierType that has been remotely persisted
      const savedPurgeEntity = compliancePurgeEntities.filterBy('identifierType', identifierType).shift();
      const label = identifierType.replace(/_/g, ' ').toLowerCase().capitalize();

      return {
        identifierType,
        label,
        identifierField: savedPurgeEntity ? savedPurgeEntity.identifierField.split(',') : []
      };
    });
  }),

  didRender() {
    const $typeahead = $('#compliance-typeahead') || [];
    if ($typeahead.length) {
      this.enableTypeaheadOn($typeahead);
    }
  },

  enableTypeaheadOn(selector) {
    selector.autocomplete({
      minLength: 0,
      source: request => {
        const {term = ''} = request;
        this.set('searchTerm', term);
      }
    });
  },

  /**
   * Returns a compliancePurgeEntity matching the given Id.
   * @param {string} id value representing the identifierType
   * @returns {*}
   */
  getPurgeEntity(id) {
    // There should be only one match in the resulting array
    return this.get('privacyCompliancePolicy.compliancePurgeEntities')
        .filterBy('identifierType', id)
        .get('firstObject');
  },

  addPurgeEntityToComplianceEntities(identifierType) {
    this.get('privacyCompliancePolicy.compliancePurgeEntities').addObject({identifierType, identifierField: ''});
    return this.getPurgeEntity(identifierType);
  },

  /**
   * Internal abstraction for adding and removing an Id from an identifierField
   * @param {string} fieldValue name of identifier to add/remove from identifier type
   * @param {string} idType the identifierType for a compliancePurgeEntity
   * @param {string} toggleOperation string representing the operation to be performed
   * @returns {boolean|*} true on success
   * @private
   */
  _togglePurgeIdOnIdentifierField(fieldValue, idType, toggleOperation) {
    function updateIdentifierFieldOn(entity, updatedValue) {
      return Ember.set(entity, 'identifierField', updatedValue);
    }

    const op = {
      /**
       * Adds the fieldValue to the specified idType if available, otherwise creates a new compliancePurgeEntity
       * @param purgeableEntityField
       * @returns {*|Object} the updated compliancePurgeEntity
       */
      add: (purgeableEntityField = this.addPurgeEntityToComplianceEntities(idType)) => {
        const currentId = purgeableEntityField.identifierField;
        const updatedIds = currentId.length ? currentId.split(',').addObject(fieldValue).join(',') : fieldValue;

        return updateIdentifierFieldOn(purgeableEntityField, updatedIds);
      },
      /**
       * Removes the fieldValue from the specified idType if available, otherwise function is no-op
       * @param purgeableEntityField
       * @returns {*|Object} the updated compliancePurgeEntity
       */
      remove: (purgeableEntityField) => {
        if (purgeableEntityField) {
          const currentId = purgeableEntityField.identifierField;
          const updatedIds = currentId.length ? currentId.split(',').removeObject(fieldValue).join(',') : '';

          return updateIdentifierFieldOn(purgeableEntityField, updatedIds);
        }
      }
    }[toggleOperation];

    return typeof op === 'function' && op(this.getPurgeEntity(idType));
  },

  actions: {
    addPurgeId(name, idType) {
      this._togglePurgeIdOnIdentifierField(name, idType, `add`);
    },

    removePurgeId(name, idType) {
      this._togglePurgeIdOnIdentifierField(name, idType, `remove`);
    },

    updateComplianceType ({value}) {
      this.set('privacyCompliancePolicy.complianceType', value);
    },

    saveCompliance () {
      this.get('onSave')();
      return false;
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetCompliance () {
      this.get('onReset')();
    }
  }
});
