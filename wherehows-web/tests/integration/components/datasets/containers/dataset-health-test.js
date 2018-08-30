import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';

module('Integration | Component | datasets/containers/dataset-health', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    defaultScenario(this.server);

    this.set('urn', urn);
    await render(hbs`{{datasets/containers/dataset-health urn=urn}}`);

    assert.ok(this.element, 'Renders without errors');
    assert.ok(find('.dataset-health__score-table'), 'renders the health table component');
  });
});
