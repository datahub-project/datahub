import {
  lastSeenSuggestionInterval,
  lowQualitySuggestionConfidenceThreshold,
  logicalTypeValueLabel,
  formatAsCapitalizedStringWithSpaces
} from 'wherehows-web/constants/metadata-acquisition';
import {
  Classification,
  nonIdFieldLogicalTypes,
  NonIdLogicalType,
  IdLogicalType
} from 'wherehows-web/constants/datasets/compliance';
import { module, test } from 'qunit';

module('Unit | Utility | datasets/metadata acquisition');

/**
 * A list of classification strings for a dataset field
 * @type {Array<Classification>}
 */
const classificationStrings = Object.values(Classification);

/**
 * A list of display string for non-id field logical types
 * @type {Array<string>}
 */
const nonIdFieldLogicalTypesDisplayStrings = Object.values(nonIdFieldLogicalTypes).map(({ displayAs }) => displayAs);

/**
 * A list of strings representing generic logical type
 * @type {Array<NonIdLogicalType>}
 */
const nonIdFieldLogicalTypeValues = Object.values(NonIdLogicalType);

/**
 * A list of expected IdLogicalType string display values
 * @type {Array<string>}
 */
const idFieldLogicalTypeDisplayStrings = ['Numeric', 'Urn', 'Reversed Urn', 'Composite Urn'];

/**
 * A list of IdLogicalType string values
 * @type {Array<IdLogicalType>}
 */
const idFieldLogicalTypeValues = Object.values(IdLogicalType);

test('lastSeenSuggestionInterval is a number', function(assert) {
  assert.ok(typeof lastSeenSuggestionInterval === 'number');
});
module('Unit | Utility | datasets/metadata acquisition');

test('lowQualitySuggestionConfidenceThreshold is a number', function(assert) {
  assert.ok(typeof lowQualitySuggestionConfidenceThreshold === 'number');
});

test('logicalTypeValueLabel generates correct labels for generic types', function(assert) {
  const labels = logicalTypeValueLabel('generic');
  labels.forEach(({ label, value }) => {
    assert.ok(
      nonIdFieldLogicalTypesDisplayStrings.includes(label),
      `Generated label ${label} found in nonIdFieldLogicalTypesDisplayStrings: ${nonIdFieldLogicalTypesDisplayStrings}`
    );
    assert.ok(nonIdFieldLogicalTypeValues.includes(value), `Value ${value} found in ${nonIdFieldLogicalTypeValues}`);
  });
});

test('logicalTypeValueLabel generates correct labels for id types', function(assert) {
  const labels = logicalTypeValueLabel('id');
  labels.forEach(({ label, value }) => {
    assert.ok(
      idFieldLogicalTypeDisplayStrings.includes(label),
      `Generated label ${label} found in idFieldLogicalTypeDisplayStrings: ${idFieldLogicalTypeDisplayStrings}`
    );

    assert.ok(idFieldLogicalTypeValues.includes(value), `Value ${value} found in ${idFieldLogicalTypeValues}`);
  });
});

test('formatAsCapitalizedStringWithSpaces generates the correct display string', function(assert) {
  [
    ['CONFIDENTIAL', 'Confidential'],
    ['LIMITED_DISTRIBUTION', 'Limited distribution'],
    ['HIGHLY_CONFIDENTIAL', 'Highly confidential']
  ].forEach(([source, target]) => {
    assert.equal(formatAsCapitalizedStringWithSpaces(source), target, `correctly converts ${source}`);
  });
});
