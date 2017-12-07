import { module, test } from 'qunit';
import { getComplianceSteps, complianceSteps } from 'wherehows-web/constants';

module('Unit | Constants | dataset compliance');

test('getComplianceSteps function should behave as expected', function(assert) {
  assert.expect(3);
  const piiTaggingStep = { 0: { name: 'editDatasetLevelCompliancePolicy' } };
  let result;

  assert.equal(typeof getComplianceSteps, 'function', 'getComplianceSteps is a function');
  result = getComplianceSteps();

  assert.deepEqual(result, complianceSteps, 'getComplianceSteps result is expected shape when no args are passed');

  result = getComplianceSteps({ hasSchema: false });
  assert.deepEqual(
    result,
    { ...complianceSteps, ...piiTaggingStep },
    'getComplianceSteps result is expected shape when hasSchema attribute is false'
  );
});
