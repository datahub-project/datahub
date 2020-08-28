import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import EntityListsManager from '@datahub/shared/services/entity-lists-manager';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';

module('Unit | Service | entity-lists-manager', function(hooks): void {
  setupTest(hooks);

  test('it adds and removes an entity', function(assert): void {
    const service: EntityListsManager = this.owner.lookup('service:entity-lists-manager');
    const testEntity = new FeatureEntity('urn');

    assert.ok(service, 'Expected entity-lists-manager to be a service');

    // Add an entity to list
    service.addToList(testEntity);

    assert.ok(Array.isArray(service.entities[FeatureEntity.displayName]), 'Expected entity list name to be an Array');
    assert.ok(service.entities[FeatureEntity.displayName].length === 1, 'Expected entity list to contain 1 entity');

    // Add same entity to list
    service.addToList(testEntity);

    assert.ok(
      service.entities[FeatureEntity.displayName].length === 1,
      'Expected adding pre-existing entity to be a noop, list length should still be 1'
    );

    // Remove entity from list
    service.removeFromList(testEntity);

    assert.ok(
      service.entities[FeatureEntity.displayName].length === 0,
      'Expected entity list to be empty when removeFromList is called with sole entity'
    );
  });

  test('it adds multiple entities and correctly groups entities', function(assert): void {
    const service: EntityListsManager = this.owner.lookup('service:entity-lists-manager');
    const testFeatures = [new FeatureEntity('feature-urn-a'), new FeatureEntity('feature-urn-b')];
    const testMetrics = [new MockEntity('metric-urn-a'), new MockEntity('metric-urn-b')];

    service.supportedListEntities = [MockEntity.displayName, FeatureEntity.displayName];

    // Add multiple entity types to list
    service.addToList([...testFeatures, ...testMetrics]);

    assert.equal(
      service.entities[FeatureEntity.displayName].length,
      testMetrics.length,
      `Expected test features to be grouped under ${FeatureEntity.displayName}`
    );
    assert.equal(
      service.entities[MockEntity.displayName].length,
      testFeatures.length,
      `Expected testMetrics to be grouped under ${MockEntity.displayName}`
    );

    // Remove a single entity from list
    service.removeFromList(testMetrics[1]);

    assert.equal(
      service.entities[MockEntity.displayName].length,
      testMetrics.length - 1,
      `Expected stored entities for ${MockEntity.displayName} to be reduced by 1`
    );

    // Re-add removed entity from list
    service.addToList(testMetrics[1]);

    assert.equal(
      service.entities[MockEntity.displayName].length,
      testMetrics.length,
      'Expected a previously removed entity to be eligible for re-addition'
    );

    // Remove multiple entities from list
    service.removeFromList(testFeatures);

    assert.equal(
      service.entities[FeatureEntity.displayName].length,
      0,
      'Expected removeFromList to remove all entities in argument list'
    );
  });
});
