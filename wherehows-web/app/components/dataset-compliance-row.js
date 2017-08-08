import Ember from 'ember';
import DatasetTableRow from 'wherehows-web/components/dataset-table-row';
import { fieldIdentifierTypes, defaultFieldDataTypeClassification } from 'wherehows-web/constants';
import {
  fieldIdentifierTypeIds,
  logicalTypesForIds,
  logicalTypesForGeneric
} from 'wherehows-web/components/dataset-compliance';

const { computed, get } = Ember;

const isMixedId = identifierType => identifierType === fieldIdentifierTypes.generic.value;
const isCustomId = identifierType => identifierType === fieldIdentifierTypes.custom.value;

export default DatasetTableRow.extend({
  /**
   * @type {Array} logical id types mapped to options for <select>
   */
  logicalTypesForIds,

  /**
   * @type {Array} logical generic types mapped to options for <select>
   */
  logicalTypesForGeneric,

  /**
   * flag indicating that the identifier type is a generic value
   * @type {Ember.computed<boolean>}
   */
  isMixed: computed.equal('field.identifierType', fieldIdentifierTypes.generic.value),

  /**
   * flag indicating that the identifier type is a custom value
   * @type {Ember.computed<boolean>}
   */
  isCustom: computed.equal('field.identifierType', fieldIdentifierTypes.custom.value),

  /**
   * aliases the identifierField on the field
   * @type {Ember.computed<string>}
   */
  identifierField: computed.alias('field.identifierField'),

  /**
   * aliases the data type for the field
   * @type {Ember.computed<string>}
   */
  dataType: computed.alias('field.dataType'),

  /**
   * Checks if the field format drop-down should be disabled based on the type of the field
   * @type {Ember.computed}
   */
  isFieldFormatDisabled: computed('field.identifierType', function() {
    const identifierType = get(this, 'field.identifierType');
    return isMixedId(identifierType) || isCustomId(identifierType);
  }).readOnly(),

  /**
   * Returns a computed value for the field identifierType
   * isSubject flag on the field is represented as MemberId(Subject Member Id)
   * @type {Ember.computed<string>}
   */
  identifierType: computed('field.identifierType', 'field.isSubject', function() {
    const identifierType = get(this, 'field.identifierType');

    return identifierType === fieldIdentifierTypes.member.value && get(this, 'field.isSubject')
      ? fieldIdentifierTypes.subjectMember.value
      : identifierType;
  }).readOnly(),

  /**
   * A list of field formats that are determined based on the field identifierType
   * @type {Ember.computed<Array>}
   */
  fieldFormats: computed('field.identifierType', function() {
    const identifierType = get(this, 'field.identifierType');
    const logicalTypesForIds = get(this, 'logicalTypesForIds');

    const mixed = isMixedId(identifierType);
    const custom = isCustomId(identifierType);

    let fieldFormats = fieldIdentifierTypeIds.includes(identifierType)
      ? logicalTypesForIds
      : get(this, 'logicalTypesForGeneric');
    const urnFieldFormat = logicalTypesForIds.findBy('value', 'URN');
    fieldFormats = mixed ? urnFieldFormat : fieldFormats;
    fieldFormats = custom ? void 0 : fieldFormats;

    return fieldFormats;
  }),

  /**
   * The fields logical type, rendered as an Object
   * @type {Ember.computed<Object>}
   */
  logicalType: computed('field.logicalType', function() {
    const fieldFormats = get(this, 'fieldFormats');
    const logicalType = get(this, 'field.logicalType');

    // Same object reference for equality comparision
    return Array.isArray(fieldFormats) ? fieldFormats.findBy('value', logicalType) : fieldFormats;
  }),

  /**
   * The field security classification
   * @type {Ember.computed}
   */
  classification: computed('field.classification', 'field.identifierType', function() {
    const identifierType = get(this, 'field.identifierType');
    const mixed = isMixedId(identifierType);
    // Filtered list of id logical types that end with urn, or have no value
    const urnFieldFormat = get(this, 'logicalTypesForIds').findBy('value', 'URN');

    return get(this, 'field.classification') || (mixed && defaultFieldDataTypeClassification[urnFieldFormat.value]);
  }),

  actions: {
    /**
     * Handles UI changes to the field identifierType
     * @param {string} value
     */
    onFieldIdentifierTypeChange({ value }) {
      const { onFieldIdentifierTypeChange } = this.attrs;
      if (typeof onFieldIdentifierTypeChange === 'function') {
        onFieldIdentifierTypeChange(get(this, 'field'), { value });
      }
    },

    /**
     * Handles the updates when the field logical type changes on this field
     * @param {Event|null} e
     */
    onFieldLogicalTypeChange(e) {
      const { value } = e || {};
      const { onFieldLogicalTypeChange } = this.attrs;
      if (typeof onFieldLogicalTypeChange === 'function') {
        onFieldLogicalTypeChange(get(this, 'field'), { value });
      }
    },

    /**
     * Handles UI change to field security classification
     * @param {string} value
     */
    onFieldClassificationChange({ value }) {
      const { onFieldClassificationChange } = this.attrs;
      if (typeof onFieldClassificationChange === 'function') {
        onFieldClassificationChange(get(this, 'field'), { value });
      }
    }
  }
});
