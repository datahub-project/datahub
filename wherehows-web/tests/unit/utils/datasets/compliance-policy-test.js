import {
  createInitialComplianceInfo,
  fieldChangeSetRequiresReview
} from 'wherehows-web/utils/datasets/compliance-policy';

import { mockFieldChangeSets } from 'wherehows-web/tests/helpers/datasets/compliance-policy/field-changeset-constants';
import { module, test } from 'qunit';

module('Unit | Utility | datasets/compliance policy');

test('Utility function createInitialComplianceInfo exists', function(assert) {
  assert.expect(2);
  const mockId = 1337;
  const initialComplianceInfo = {
    datasetId: mockId,
    complianceType: 'AUTO_PURGE',
    complianceEntities: [],
    fieldClassification: {},
    datasetClassification: {},
    geographicAffinity: { affinity: '' },
    recordOwnerType: '',
    retentionPolicy: { retentionType: '' }
  };

  assert.ok(typeof createInitialComplianceInfo === 'function', 'createInitialComplianceInfo is a function');
  assert.deepEqual(createInitialComplianceInfo(mockId), initialComplianceInfo, 'generates policy in expected shape');
});

test('Compliance utility function fieldChangeSetRequiresReview exists', function(assert) {
  assert.ok(typeof fieldChangeSetRequiresReview === 'function', 'fieldChangeSetRequiresReview is a function');

  assert.ok(typeof fieldChangeSetRequiresReview() === 'boolean', 'fieldChangeSetRequiresReview returns a boolean');
});

test('Compliance utility function fieldChangeSetRequiresReview exists', function(assert) {
  assert.expect(mockFieldChangeSets.length);

  mockFieldChangeSets.forEach(changeSet =>
    assert.ok(fieldChangeSetRequiresReview(changeSet) === changeSet.__requiresReview__, changeSet.__msg__)
  );
});
