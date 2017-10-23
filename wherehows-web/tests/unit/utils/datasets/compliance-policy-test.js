import {
  createInitialComplianceInfo,
  fieldChangeSetRequiresReview,
  isRecentSuggestion
} from 'wherehows-web/utils/datasets/compliance-policy';

import { mockFieldChangeSets } from 'wherehows-web/tests/helpers/datasets/compliance-policy/field-changeset-constants';
import { mockTimeStamps } from 'wherehows-web/tests/helpers/datasets/compliance-policy/recent-suggestions-constants';
import { module, test } from 'qunit';

module('Unit | Utility | datasets/compliance policy');

test('Utility function createInitialComplianceInfo exists', function(assert) {
  assert.expect(2);
  const mockId = 1337;
  const initialComplianceInfo = {
    datasetId: mockId,
    complianceType: '',
    compliancePurgeNote: '',
    complianceEntities: [],
    datasetClassification: {}
  };

  assert.ok(typeof createInitialComplianceInfo === 'function', 'createInitialComplianceInfo is a function');
  assert.deepEqual(createInitialComplianceInfo(mockId), initialComplianceInfo, 'generates policy in expected shape');
});

test('Compliance utility function fieldChangeSetRequiresReview exists', function(assert) {
  assert.ok(typeof fieldChangeSetRequiresReview === 'function', 'fieldChangeSetRequiresReview is a function');

  assert.ok(typeof fieldChangeSetRequiresReview() === 'boolean', 'fieldChangeSetRequiresReview returns a boolean');
});

test('fieldChangeSetRequiresReview correctly determines if a fieldChangeSet requires review', function(assert) {
  assert.expect(mockFieldChangeSets.length);

  mockFieldChangeSets.forEach(changeSet =>
    assert.ok(fieldChangeSetRequiresReview(changeSet) === changeSet.__requiresReview__, changeSet.__msg__)
  );
});

test('isRecentSuggestion exists', function(assert) {
  assert.expect(1);
  assert.ok(typeof isRecentSuggestion === 'function', 'isRecentSuggestion is a function');
});

test('isRecentSuggestion correctly determines if a suggestion is recent or not', function(assert) {
  assert.expect(mockTimeStamps.length);

  mockTimeStamps.forEach(({ policyModificationTime, suggestionModificationTime, __isRecent__, __assertMsg__ }) => {
    const recent = isRecentSuggestion(policyModificationTime, suggestionModificationTime);
    assert.ok(recent === __isRecent__, `${__assertMsg__} isRecent? ${recent}`);
  });
});
