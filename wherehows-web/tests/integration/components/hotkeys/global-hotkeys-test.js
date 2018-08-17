import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | hotkeys/global-hotkeys', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    // Set any properties with this.set('myProperty', 'value');
    // Handle any actions with this.set('myAction', function(val) { ... });

    await render(hbs`{{hotkeys/global-hotkeys}}`);

    assert.equal(this.element.textContent.trim(), '');

    // Template block usage:
    await render(hbs`
      {{#hotkeys/global-hotkeys}}
        template block text
      {{/hotkeys/global-hotkeys}}
    `);

    assert.equal(this.element.textContent.trim(), 'template block text');
  });
});
