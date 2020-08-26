import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import DataModelsService from '@datahub/data-models/services/data-models';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { setProperties, set } from '@ember/object';
import { ICorpUserInfo } from '@datahub/metadata-types/types/entity/person/person-entity';
import TestDataModelsService from '@datahub/data-models/mirage-addon/test-helpers/test-data-models-service';
import { TestPersonEntity } from '@datahub/data-models/mirage-addon/test-helpers/test-entities/test-person-entity';

module('Unit | Service | data-models', function(hooks): void {
  setupTest(hooks);

  test('it exists', function(assert): void {
    const service = this.owner.lookup('service:data-models');
    assert.ok(service);
  });

  test('it returns the correct entity', function(assert): void {
    const service: DataModelsService = this.owner.lookup('service:data-models');
    const datasetEntity = service.getModel(DatasetEntity.displayName);

    assert.equal(datasetEntity.kind, DatasetEntity.kind);
  });

  test('it creates relationships as expected', function(assert): void {
    this.owner.register('service:data-models', TestDataModelsService);
    const service: DataModelsService = this.owner.lookup('service:data-models');
    const mockPerson = service.createPartialInstance('people', 'pikachu') as TestPersonEntity;

    assert.ok(mockPerson instanceof PersonEntity, 'Created the instance as expected');
    assert.ok(mockPerson instanceof TestPersonEntity);
    setProperties(mockPerson, {
      manualDirectReportUrns: ['charmander', 'bulbasaur', 'squirtle'],
      manualPeersUrns: ['eevee']
    });
    assert.equal(mockPerson.directReports.length, 3, 'Created 3 instances for direct reports');
    assert.ok(mockPerson.directReports[0] instanceof PersonEntity, 'Created the correct kind of relationship instance');
    assert.equal(
      mockPerson.directReports[1].urn,
      'bulbasaur',
      'Properly created a person entity of with the right urn'
    );
    assert.equal(mockPerson.peers.length, 1, 'Multiple relationships are displayed correctly');
    assert.equal(
      mockPerson.peers[0].urn,
      'eevee',
      'The correct object is created when dealing with multiple relationships'
    );

    const mockPersonEntity = {
      info: {
        managerUrn: 'aketchum',
        managerName: 'Ash Ketchum'
      }
    } as ICorpUserInfo;
    set(mockPerson, 'entity', mockPersonEntity);

    assert.ok(mockPerson.reportsTo instanceof PersonEntity, 'Works with a relationship to a singer urn');

    assert.equal(
      (mockPerson.reportsTo as PersonEntity).name,
      'Ash Ketchum',
      'Created the expected single entity relationship'
    );
  });
});
