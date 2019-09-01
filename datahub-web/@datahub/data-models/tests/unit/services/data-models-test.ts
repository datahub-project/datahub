import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import DataModelsService from '@datahub/data-models/services/data-models';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

module('Unit | Service | data-models', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    const service = this.owner.lookup('service:data-models');
    assert.ok(service);
  });

  test('it returns the correct entity', function(assert) {
    const service: DataModelsService = this.owner.lookup('service:data-models');
    const datasetEntity = service.getModel(DatasetEntity.displayName);

    assert.equal(datasetEntity.kind, DatasetEntity.kind);
  });
});
