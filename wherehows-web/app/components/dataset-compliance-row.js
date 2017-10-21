import Ember from 'ember';
import DatasetTableRow from 'wherehows-web/components/dataset-table-row';
import {
  fieldIdentifierTypeIds,
  fieldIdentifierOptions,
  defaultFieldDataTypeClassification,
  isMixedId,
  isCustomId,
  hasPredefinedFieldFormat,
  logicalTypesForIds,
  logicalTypesForGeneric,
  SuggestionIntent
} from 'wherehows-web/constants';
import { fieldChangeSetRequiresReview } from 'wherehows-web/utils/datasets/compliance-policy';
import { isHighConfidenceSuggestion } from 'wherehows-web/utils/datasets/compliance-suggestions';
import { hasEnumerableKeys } from 'wherehows-web/utils/object';

const { computed, get, getProperties, getWithDefault } = Ember;

export default DatasetTableRow.extend({
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
   * aliases the suggestionAuthority field property if present
   * @type {Ember.computed}
   */
  suggestionAuthority: computed.alias('field.suggestionAuthority'),

  /**
   * Checks that the field does not have a current policy value
   * @type {Ember.computed}
   * @return {boolean}
   */
  isReviewRequested: computed('field.{isDirty,suggestion,privacyPolicyExists,suggestionAuthority}', function() {
    return fieldChangeSetRequiresReview(get(this, 'field'));
  }),

  /**
   * Maps the suggestion response to a string resolution
   * @type {Ember.computed}
   * @return {string|void}
   */
  suggestionResolution: computed('field.suggestionAuthority', function() {
    return {
      [SuggestionIntent.accept]: 'Accepted',
      [SuggestionIntent.ignore]: 'Discarded'
    }[get(this, 'field.suggestionAuthority')];
  }),

  /**
   * Checks if the field format drop-down should be disabled based on the type of the field
   * @type {Ember.computed}
   */
  isFieldFormatDisabled: computed('field.identifierType', function() {
    return hasPredefinedFieldFormat(get(this, 'field.identifierType'));
  }).readOnly(),

  /**
   * Takes a field property and extracts the value on the current policy if a suggestion currently exists for the field
   * @param {string} fieldProp the field property, either logicalType or identifierType to pick from the field
   * @return {string | void} the current value if a suggestion & current value exists or undefined
   */
  getCurrentValueBeforeSuggestion(fieldProp) {
    if (hasEnumerableKeys(get(this, 'prediction'))) {
      /**
       * Current value on policy prior to the suggested value
       * @type {string}
       */
      const value = get(this, `field.${fieldProp}`);

      /**
       * Convenience function to get `label` attribute on the display properties object
       * @param {Array<{value: string, label: string}>} valueObjects
       */
      const getLabel = (valueObjects = []) =>
        Array.isArray(valueObjects) && (valueObjects.findBy('value', value) || {}).label;

      return {
        identifierType: getLabel(fieldIdentifierOptions),
        logicalType: getLabel(get(this, 'fieldFormats'))
      }[fieldProp];
    }
  },

  /**
   * Returns a computed value for the field identifierType
   * @type {Ember.computed<string>}
   */
  identifierType: computed('field.identifierType', 'prediction', function() {
    const identifierTypePath = 'field.identifierType';
    /**
     * Inner function takes the field.identifierType and prediction values, and
     * returns the identifierType to be rendered in the ui
     * @param params
     * @return {*}
     */
    const getIdentifierType = params => {
      const {
        [identifierTypePath]: identifierType,
        prediction: { identifierType: suggestedIdentifierType } = {}
      } = params;
      return suggestedIdentifierType || identifierType;
    };

    return getIdentifierType(getProperties(this, [identifierTypePath, 'prediction']));
  }).readOnly(),

  /**
   * Gets the identifierType on the compliance policy before the suggested value
   * @type {string | void}
   */
  identifierTypeBeforeSuggestion: computed('identifierType', function() {
    return this.getCurrentValueBeforeSuggestion('identifierType');
  }),

  /**
   * Gets the logicalType on the compliance policy before the suggested value
   * @type {string | void}
   */
  logicalTypeBeforeSuggestion: computed('logicalType', function() {
    return this.getCurrentValueBeforeSuggestion('logicalType');
  }),

  /**
   * A list of field formats that are determined based on the field identifierType
   * @type {Ember.computed<Array>}
   */
  fieldFormats: computed('field.identifierType', function() {
    const identifierType = get(this, 'field.identifierType');
    const urnFieldFormat = logicalTypesForIds.findBy('value', 'URN');

    const mixed = isMixedId(identifierType);
    const custom = isCustomId(identifierType);
    let fieldFormats = fieldIdentifierTypeIds.includes(identifierType) ? logicalTypesForIds : logicalTypesForGeneric;

    fieldFormats = mixed ? urnFieldFormat : fieldFormats;
    fieldFormats = custom ? void 0 : fieldFormats;

    return fieldFormats;
  }),

  /**
   * The fields logical type, rendered as an Object
   * If a prediction exists for this field, the predicted value is shown instead
   * @type {Ember.computed<Object>}
   */
  logicalType: computed('field.logicalType', 'prediction', 'fieldFormats', function() {
    const logicalTypePath = 'field.logicalType';
    let {
      fieldFormats,
      [logicalTypePath]: logicalType,
      prediction: { logicalType: suggestedLogicalType } = {}
    } = getProperties(this, ['fieldFormats', logicalTypePath, 'prediction']);

    suggestedLogicalType && (logicalType = suggestedLogicalType);

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
    const urnFieldFormat = logicalTypesForIds.findBy('value', 'URN');

    return get(this, 'field.classification') || (mixed && defaultFieldDataTypeClassification[urnFieldFormat.value]);
  }),

  /**
   * Extracts the field suggestions into a cached computed property, if a suggestion exists
   * @type {Ember.ComputedProperty}
   * @return {void | {identifierType: string, logicalType: string, confidence: number}}
   */
  prediction: computed('field.suggestion', 'field.suggestionAuthority', function() {
    const field = getWithDefault(this, 'field', {});
    // If a suggestionAuthority property exists on the field, then the user has already either accepted or ignored
    // the suggestion for this field. It's value should not be take into account on re-renders
    // this line takes that into account and substitutes an empty suggestion
    const { suggestion } = field.hasOwnProperty('suggestionAuthority') ? {} : field;

    if (suggestion && isHighConfidenceSuggestion(suggestion)) {
      const { identifierType, logicalType, confidenceLevel: confidence } = suggestion;

      return { identifierType, logicalType, confidence: +(confidence * 100).toFixed(2) };
    }
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
    },

    /**
     * Handler for user interactions with a suggested value. Applies / ignores the suggestion
     * Then invokes the parent supplied suggestion handler
     * @param {string | void} intent a binary indicator to accept or ignore suggestion
     */
    onSuggestionAction(intent) {
      const { onSuggestionIntent } = this.attrs;

      // Accept the suggestion for either identifierType and/or logicalType
      if (intent === SuggestionIntent.accept) {
        const { identifierType, logicalType } = get(this, 'prediction');
        if (identifierType) {
          this.actions.onFieldIdentifierTypeChange.call(this, { value: identifierType });
        }

        if (logicalType) {
          this.actions.onFieldLogicalTypeChange.call(this, { value: logicalType });
        }
      }

      // Invokes parent handle to  runtime ignore future suggesting this suggestion
      if (typeof onSuggestionIntent === 'function') {
        onSuggestionIntent(get(this, 'field'), intent);
      }
    }
  }
});
