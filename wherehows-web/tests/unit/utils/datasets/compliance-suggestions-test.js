import { module, test } from 'qunit';
import { getFieldSuggestions, isHighConfidenceSuggestion } from 'wherehows-web/utils/datasets/compliance-suggestions';
import { lowQualitySuggestionConfidenceThreshold, SuggestionIntent } from 'wherehows-web/constants';

module('Unit | Utility | datasets/compliance suggestions');

test('isHighConfidenceSuggestion correctly determines the confidence of a suggestion', function(assert) {
  let result = isHighConfidenceSuggestion({});
  assert.notOk(result, 'should be false if no arguments are supplied');

  result = isHighConfidenceSuggestion({ confidenceLevel: lowQualitySuggestionConfidenceThreshold + 1 });

  assert.ok(
    result,
    `should be true if the confidence value is greater than ${lowQualitySuggestionConfidenceThreshold}`
  );

  result = isHighConfidenceSuggestion({ confidenceLevel: lowQualitySuggestionConfidenceThreshold - 1 });

  assert.notOk(
    result,
    `should be false if the confidence value is less than ${lowQualitySuggestionConfidenceThreshold}`
  );

  result = isHighConfidenceSuggestion({ confidenceLevel: lowQualitySuggestionConfidenceThreshold });

  assert.notOk(
    result,
    `should be false if the confidence value is equal to ${lowQualitySuggestionConfidenceThreshold}`
  );
});

test('getFieldSuggestions correctly extracts suggestions from a compliance field', function(assert) {
  let changeSetField = {
    suggestion: {
      identifierType: '',
      logicalType: '',
      securityClassification: '',
      confidenceLevel: 1
    },

    suggestionAuthority: SuggestionIntent.accept
  };

  let result = getFieldSuggestions({});

  assert.ok(typeof result === 'undefined', 'expected undefined return when the argument is an empty object');

  result = getFieldSuggestions();

  assert.ok(typeof result === 'undefined', 'expected undefined return when no argument is supplied');

  result = getFieldSuggestions({ suggestion: changeSetField.suggestion });

  assert.deepEqual(
    result,
    {
      identifierType: changeSetField.suggestion.identifierType,
      logicalType: changeSetField.suggestion.logicalType,
      confidence: 100.0
    },
    'expected suggestions to match changeSetField properties'
  );

  result = getFieldSuggestions(changeSetField);

  assert.expect(
    typeof result === 'undefined',
    'expected undefined return when suggestion Authority exists on changSetField'
  );
});
