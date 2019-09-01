import { unGuardedEntities } from 'wherehows-web/utils/entity/flag-guard';
import { module, skip } from 'qunit';
import Configurator, { setMockConfig, resetConfig } from 'wherehows-web/services/configurator';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

module('Unit | Utility | entity/flag-guard', function(hooks) {
  hooks.beforeEach(function() {
    setMockConfig({ showFeatures: true, showUmp: true });
  });

  hooks.afterEach(function() {
    resetConfig();
  });

  skip('unGuardedEntities behaves as expected', function(assert) {
    const configurator = Configurator;
    const numOfEntities = Object.values(DataModelEntity).length;
    let entities = unGuardedEntities(configurator);

    assert.ok(Array.isArray(entities), `Expected entities to be of type array`);

    assert.equal(entities.length, numOfEntities, `Expected to find ${numOfEntities} entities unguarded`);

    assert.deepEqual(
      entities.map(({ displayName }: DataModelEntity): string => displayName).sort(),
      Object.keys(DataModelEntity).sort(),
      `Expected all elements to be DataModelEntities`
    );

    setMockConfig({ showFeatures: false, showUmp: true });

    entities = unGuardedEntities(configurator);

    const updatedNumOfEntities = numOfEntities - 1;
    assert.equal(
      entities.length,
      updatedNumOfEntities,
      `Expected to find ${updatedNumOfEntities} entities after guarding features`
    );
  });
});
