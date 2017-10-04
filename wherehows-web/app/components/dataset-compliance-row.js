import Ember from 'ember';
import DatasetTableRow from 'wherehows-web/components/dataset-table-row';
import {
  fieldIdentifierTypeIds,
  defaultFieldDataTypeClassification,
  isMixedId,
  isCustomId,
  hasPredefinedFieldFormat,
  logicalTypesForIds,
  logicalTypesForGeneric,
  SuggestionIntent
} from 'wherehows-web/constants';
import { fieldChangeSetRequiresReview } from 'wherehows-web/utils/datasets/compliance-policy';
import { compact } from 'wherehows-web/utils/array';
import {
  highConfidenceSuggestions,
  accumulateFieldSuggestions
} from 'wherehows-web/utils/datasets/compliance-suggestions';

const { computed, get, getProperties } = Ember;

/**
 * Extracts the suggestions for identifierType, logicalType suggestions, and confidence from a list of predictions
 * The last item in the list holds the highest precedence
 * @param {Array<Object>} predictions
 * @returns Array<Object>
 */
const getFieldSuggestions = predictions => accumulateFieldSuggestions(highConfidenceSuggestions(compact(predictions)));

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
    const prediction = get(this, 'prediction') || {};
    const suggested = prediction[fieldProp];
    const { label: current } = get(this, fieldProp) || {};

    if (suggested !== 'undefined' && current) {
      return current;
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
   * @type {Ember.computed}
   */
  prediction: computed('field.suggestion', 'field.suggestionAuthority', 'hasRecentSuggestions', function() {
    const { field = {}, hasRecentSuggestions } = getProperties(this, 'field', 'hasRecentSuggestions');
    // If a suggestionAuthority property exists on the field, then the user has already either accepted or ignored
    // the suggestion for this field. It's value should not be take into account on re-renders
    // this line takes that into account and substitutes an empty suggestion
    const { suggestion } = field.hasOwnProperty('suggestionAuthority') ? {} : field;

    if (suggestion && hasRecentSuggestions) {
      const { identifierTypePrediction, logicalTypePrediction } = suggestion;
      // The order of the array supplied to getFieldSuggestions is importance to it's order of operations
      // the last element in the array takes highest precedence: think Object.assign
      const { identifierType, logicalType, confidence } = getFieldSuggestions([
        logicalTypePrediction,
        identifierTypePrediction
      ]);

      return { identifierType, logicalType, confidence };
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
