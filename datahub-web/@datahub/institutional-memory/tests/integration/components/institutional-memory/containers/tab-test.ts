import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | institutional-memory/containers/tab', function(hooks): void {
  setupRenderingTest(hooks);
  // THis component is tested more deeply in acceptance testing since as a container it is
  // very data interaction dependent
  test('it renders', async function(assert): Promise<void> {
    await render(hbs`{{institutional-memory/containers/tab}}`);
    assert.ok(this.element, 'Initial render is without errors');
  });
});
