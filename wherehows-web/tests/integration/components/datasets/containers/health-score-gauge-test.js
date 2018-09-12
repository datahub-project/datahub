import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import defaultScenario from 'wherehows-web/mirage/scenarios/default';

module('Integration | Component | datasets/containers/health-score-gauge', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    defaultScenario(this.server);

    await render(hbs`{{datasets/containers/health-score-gauge}}`);

    assert.ok(this.element, 'Renders without errors');
  });
});
