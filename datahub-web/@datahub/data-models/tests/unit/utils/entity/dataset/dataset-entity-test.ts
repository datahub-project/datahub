import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import Sinon from 'sinon';
import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import DatasetFactory from '@datahub/data-models/mirage-addon/factories/dataset';

module('Unit | Utility | entity/dataset/dataset-entity', function(hooks): void {
  setupTest(hooks);

  test('DatasetEntity class', function(assert): void {
    const entity = new DatasetEntity('squirtle_urn');
    assert.ok(entity instanceof BaseEntity, 'Expects entity to be an instance of the BaseEntity');
    assert.equal(entity.displayName, 'datasets', 'expected entity display name to be Datasets');
    assert.equal(entity.kind, 'DatasetEntity', 'expected entity kind to be DatasetEntity');
    assert.equal(entity.urn, 'squirtle_urn', 'expected entity urn to match supplied value squirtle_urn');
  });

  sinonTest('DatasetEntity readEntity', async function(this: SinonTestContext, assert): Promise<void> {
    assert.expect(1);
    const factory = new DatasetFactory<Com.Linkedin.Dataset.Dataset>();
    const testUrn = 'urn:li:dataset:(urn:li:dataPlatform:platform1,somedir.domedata,PROD)';
    const testEntity = { ...factory.build(1), uri: testUrn };
    const requester = this.sandbox.useFakeXMLHttpRequest();
    let request: Sinon.SinonFakeXMLHttpRequest | undefined;

    requester.onCreate = (req): void => {
      request = req;
    };

    const entityRequest = new DatasetEntity(testUrn).readEntity;
    request && request.respond(200, { 'Content-Type': 'application/json' }, JSON.stringify(testEntity));
    const entity = await entityRequest;

    assert.equal(entity?.uri, testUrn, 'expected entity urn to match requested urn');
    requester.restore();
  });
});
