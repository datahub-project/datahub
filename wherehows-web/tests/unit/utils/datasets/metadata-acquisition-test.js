import { lowQualitySuggestionConfidenceThreshold } from 'wherehows-web/constants/metadata-acquisition';
import { formatAsCapitalizedStringWithSpaces } from 'wherehows-web/utils/helpers/string';
import { module, test } from 'qunit';

module('Unit | Utility | datasets/metadata acquisition', function() {
  test('lowQualitySuggestionConfidenceThreshold is a number', function(assert) {
    assert.ok(typeof lowQualitySuggestionConfidenceThreshold === 'number');
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
});
