import Ember from 'ember';
import DatasetTableRow from 'wherehows-web/components/dataset-table-row';
import { fieldIdentifierTypes, defaultFieldDataTypeClassification } from 'wherehows-web/constants';
import {
  fieldIdentifierTypeIds,
  logicalTypesForIds,
  logicalTypesForGeneric
} from 'wherehows-web/components/dataset-compliance';

const { computed, get, getProperties } = Ember;
/**
 * String indicating that the user affirms a suggestion
 * @type {string}
 */
const acceptIntent = 'accept';

/**
 * String indicating that the user ignored a suggestion
 * @type {string}
 */
const ignoreIntent = 'ignore';

/**
 * Checks if the identifierType is a mixed Id
 * @param {string} identifierType
 */
const isMixedId = identifierType => identifierType === fieldIdentifierTypes.generic.value;
/**
 * Checks if the identifierType is a custom Id
 * @param {string} identifierType
 */
const isCustomId = identifierType => identifierType === fieldIdentifierTypes.custom.value;

/**
 * Caches a list of fieldIdentifierTypes values
 * @type {any[]}
 */
const fieldIdentifierTypeValues = Object.keys(fieldIdentifierTypes)
  .map(fieldIdentifierType => fieldIdentifierTypes[fieldIdentifierType])
  .mapBy('value');

/**
 * Extracts the suggestions for identifierType, logicalType suggestions, and confidence from a list of predictions
 * The last item in the list holds the highest precedence
 * @param {Array<Object>} predictions
 * @returns Array<Object>
 */
const getFieldSuggestions = predictions =>
  predictions.filter(prediction => prediction).reduce((suggested, { value, confidence = 0 }) => {
    if (value) {
      if (fieldIdentifierTypeValues.includes(value)) {
        suggested = { ...suggested, identifierType: value };
      } else {
        suggested = { ...suggested, logicalType: value };
      }

      return {
        ...suggested,
        // value is Percent. identifierType value should be the last element in the list
        confidence: (confidence * 100).toFixed(2)
      };
    }

    return suggested;
  }, {});

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
   * aliases the suggestionAuthority field property if present
   * @type {Ember.computed}
   */
  suggestionAuthority: computed.alias('field.suggestionAuthority'),

  /**
   * Maps the suggestion response to a string resolution
   * @type {Ember.computed}
   */
  suggestionResolution: computed('field.suggestionAuthority', function() {
    return {
      [acceptIntent]: 'Accepted',
      [ignoreIntent]: 'Discarded'
    }[get(this, 'field.suggestionAuthority')];
  }),

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
   * @type {Ember.computed<string>}
   */
  identifierType: computed('field.identifierType', 'prediction', function() {
    const identifierTypePath = 'field.identifierType';
    const {
      [identifierTypePath]: identifierType,
      prediction: { identifierType: suggestedIdentifierType } = {}
    } = getProperties(this, [identifierTypePath, 'prediction']);

    return suggestedIdentifierType || identifierType;
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
   * If a prediction exists for this field, the predicted value is shown instead
   * @type {Ember.computed<Object>}
   */
  logicalType: computed('field.logicalType', 'prediction', function() {
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
    const urnFieldFormat = get(this, 'logicalTypesForIds').findBy('value', 'URN');

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
      if (intent === acceptIntent) {
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
