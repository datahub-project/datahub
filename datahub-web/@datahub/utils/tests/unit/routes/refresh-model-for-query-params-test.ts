import { module, test } from 'qunit';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';

module('Unit | Utility | route/refresh-model-for-query-params', function() {
  test('it returns the expected object for a list of query params', function(assert): void {
    const testParams = ['name', 'type', 'moveset'];
    const result = refreshModelForQueryParams(testParams);

    assert.ok(result, 'Function runs without errors');
    assert.equal(result.name.refreshModel, true, 'Correctly creates proper object interface');

    assert.equal(result.moveset.refreshModel, true, 'Correctly creates interface for multiple properties');
  });
});
