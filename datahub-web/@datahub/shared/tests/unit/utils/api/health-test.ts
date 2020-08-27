import { entityHealthUrl, healthEndpoint, getOrRecalculateHealth } from '@datahub/shared/api/health';
import { module, test } from 'qunit';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { setupRenderingTest } from 'ember-qunit';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getEntityHealth } from '@datahub/shared/mirage-addon/test-helpers/entity-health/health-metadata';
import healthScenario from '@datahub/shared/mirage-addon/scenarios/health-metadata';

module('Unit | Utility | api/health', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('it works', function(assert): void {
    const result = entityHealthUrl('');
    assert.ok(
      result.endsWith(healthEndpoint),
      'Expected the health endpoint url generator to end with the health endpoint'
    );
  });

  test('404 handling', async function(this: MirageTestContext, assert): Promise<void> {
    healthScenario(this.server);

    this.server.get(`:entity/:urn/${healthEndpoint}`, () => ({}), 404);
    this.server.post(`:entity/:urn/${healthEndpoint}`, getEntityHealth, 200);

    const result = await getOrRecalculateHealth((MockEntity as unknown) as DataModelEntity, 'urn', false);

    assert.ok(
      Array.isArray(result.validations) && result.hasOwnProperty('score'),
      'Expected the recalculation endpoint to be called when a GET 404 is encountered'
    );
  });
});
