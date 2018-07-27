import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | datasets/containers/dataset-health', function(hooks) {
  setupRenderingTest(hooks);
  // TODO: More meaningful tests as we continue with development
  test('it renders', async function(assert) {
    await render(hbs`{{datasets/containers/dataset-health}}`);
    assert.ok(this.element, 'Renders without errors');
  });
});
