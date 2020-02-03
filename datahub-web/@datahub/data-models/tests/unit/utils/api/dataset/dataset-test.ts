import { setupTest } from 'ember-qunit';
import { module } from 'qunit';
import test from 'ember-sinon-qunit/test-support/test';
import { setupSinonTest } from 'dummy/tests/helpers/setup-sinon-test';
import { readDataset } from '@datahub/data-models/api/dataset/dataset';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';
import { mockDatasetEntity } from 'dummy/constants/mocks/dataset';

module('Unit | Utility | api/dataset/dataset', function(hooks) {
  setupTest(hooks);

  test('readDataset() is a thennable', async function(this: SinonTestContext, assert) {
    assert.expect(2);
    const testDataset: IDatasetEntity = mockDatasetEntity();
    const setupValue = setupSinonTest(this);

    const response = readDataset('urn');

    assert.ok(typeof response.then === 'function', 'expected readDataset invocation to return a promise');

    setupValue.request!.respond(200, { 'Content-Type': 'application/json' }, JSON.stringify({ dataset: testDataset }));
    const value = await response;

    assert.deepEqual(value, testDataset, 'expected value to have the shape of a Feature');

    setupValue.requester.restore();
  });
});
