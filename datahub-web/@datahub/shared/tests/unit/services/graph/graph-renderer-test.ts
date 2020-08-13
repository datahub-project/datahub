import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import GraphRenderer from '@datahub/shared/services/graph-renderer';
import { simpleGraph } from '../../../../tests/helpers/graph/graphs';

module('Unit | Service | graph-renderer', function(hooks): void {
  setupTest(hooks);

  test('it exists', async function(assert): Promise<void> {
    const service: GraphRenderer = this.owner.lookup('service:graph-renderer');
    assert.ok(service);

    const svgString = await service.render(simpleGraph);

    assert.ok(svgString.indexOf('<svg') >= 0, 'has svg element');
  });
});
