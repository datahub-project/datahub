import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

import sinonTest from 'ember-sinon-qunit/test-support/test';
import Sinon from 'sinon';

module('Unit | Utility | entity/feature/feature-entity', function(hooks): void {
  setupTest(hooks);

  test('FeatureEntity class', function(assert): void {
    const entity = new FeatureEntity('test_urn');

    assert.ok(entity instanceof BaseEntity, 'expected FeatureEntity to be an instance of BaseEntity');
    assert.equal(entity.displayName, 'ml-features', 'expected entity display name to be Features');
    assert.equal(entity.kind, 'FeatureEntity', 'expected entity kind to be FeatureEntity');
    assert.equal(entity.urn, 'test_urn', 'expected entity urn to match supplied value test_urn');
  });

  sinonTest('FeatureEntity readEntity', async function(this: SinonTestContext, assert) {
    assert.expect(1);

    const testUrn = 'test_urn';
    const testEntity = { urn: testUrn };
    const requester = this.sandbox.useFakeXMLHttpRequest();
    let request: Sinon.SinonFakeXMLHttpRequest | undefined;

    requester.onCreate = req => {
      request = req;
    };

    const entityRequest = new FeatureEntity(testUrn).readEntity;
    request && request.respond(200, { 'Content-Type': 'application/json' }, JSON.stringify(testEntity));
    const entity = await entityRequest;

    assert.equal(entity.urn, testUrn, 'expected entity urn to match requested urn');
    requester.restore();
  });
});
