import Ember from 'ember';

export default Ember.Component.extend({
  searchTerm: '',
  retention: Ember.computed.alias('securitySpecification.retentionPolicy.retentionType'),
  geographicAffinity: Ember.computed.alias('securitySpecification.geographicAffinity.affinity'),
  recordOwnerType: Ember.computed.alias('securitySpecification.recordOwnerType'),
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

  didRender() {
    const $typeahead = $('#confidential-typeahead') || [];
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

  get recordOwnerTypes() {
    return ['MEMBER', 'CUSTOMER', 'JOINT', 'INTERNAL', 'COMPANY'].map(ownerType => ({
      value: ownerType,
      label: ownerType.toLowerCase().capitalize()
    }));
  },

  get retentionTypes() {
    return ['LIMITED', 'LEGAL_HOLD', 'UNLIMITED'].map(retention => ({
      value: retention,
      label: retention.replace('_', ' ').toLowerCase().capitalize()
    }));
  },

  get affinityTypes() {
    return ['LIMITED', 'EXCLUDED'].map(affinity => ({
      value: affinity,
      label: affinity.toLowerCase().capitalize()
    }));
  },

  classification: Ember.computed('securitySpecification.classification', function () {
    const confidentialClassification = this.get('securitySpecification.classification');
    const formatAsCapitalizedStringWithSpaces = string => string.replace(/[A-Z]/g, match => ` ${match}`).capitalize();

    return Object.keys(confidentialClassification).map(classifier => ({
      key: classifier,
      label: formatAsCapitalizedStringWithSpaces(classifier),
      values: Ember.get(confidentialClassification, classifier)
    }));
  }),

  _toggleOnClassification(classifier, key, operation) {
    this.get(`securitySpecification.classification.${key}`)[`${operation}Object`](classifier);
  },

  actions: {
    addToClassification(classifier, classifierKey) {
      this._toggleOnClassification(classifier, classifierKey, `add`);
    },

    removeFromClassification(classifier, classifierKey) {
      this._toggleOnClassification(classifier, classifierKey, `remove`);
    },

    updateRetentionType({value}) {
      this.set('securitySpecification.retentionPolicy.retentionType', value);
    },

    updateGeographicAffinity({value}) {
      this.set('securitySpecification.geographicAffinity.affinity', value);
    },

    updateRecordOwnerType({value}) {
      this.set('securitySpecification.recordOwnerType', value);
    },

    saveSecuritySpecification () {
      this.get('onSave')();
      return false;
    },

    approveCompliance () {
      //TODO: not implemented
    },

    disapproveCompliance () {
      //TODO: not implemented
    },

    // Rolls back changes made to the compliance spec to current
    // server state
    resetSecuritySpecification () {
      this.get('onReset')();
    }
  }
});
