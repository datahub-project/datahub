import { module, test } from 'qunit';
import { getCategory } from 'wherehows-web/utils/api/datasets/health';

module('Unit | Utility | api/datasets/health', function() {
  test('extracting category from validator string works', function(assert) {
    const testSTtring = 'com.linkedin.metadata.validators.OwnershipValidator';

    assert.equal(getCategory(testSTtring), 'Ownership');
  });
});
