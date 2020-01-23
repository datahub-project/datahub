import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import Sinon from 'sinon';
import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { mockDatasetEntity } from 'dummy/constants/mocks/dataset';

module('Unit | Utility | entity/dataset/dataset-entity', function(hooks) {
  setupTest(hooks);

  test('DatasetEntity class', function(assert) {
    const entity = new DatasetEntity('squirtle_urn');
    assert.ok(entity instanceof BaseEntity, 'Expects entity to be an instance of the BaseEntity');
    assert.equal(entity.displayName, 'datasets', 'expected entity display name to be Datasets');
    assert.equal(entity.kind, 'DatasetEntity', 'expected entity kind to be DatasetEntity');
    assert.equal(entity.urn, 'squirtle_urn', 'expected entity urn to match supplied value squirtle_urn');
  });

  sinonTest('DatasetEntity readEntity', async function(this: SinonTestContext, assert) {
    assert.expect(1);

    const testUrn = 'bulbasaur_urn';
    const testEntity = mockDatasetEntity({ uri: testUrn });
    const requester = this.sandbox.useFakeXMLHttpRequest();
    let request: Sinon.SinonFakeXMLHttpRequest | undefined;

    requester.onCreate = req => {
      request = req;
    };

    const entityRequest = new DatasetEntity(testUrn).readEntity;
    request && request.respond(200, { 'Content-Type': 'application/json' }, JSON.stringify({ dataset: testEntity }));
    const entity = await entityRequest;

    assert.equal(entity.uri, testUrn, 'expected entity urn to match requested urn');
    requester.restore();
  });
});
