import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Helper | nacho-core/titleize', function(hooks) {
  setupRenderingTest(hooks);
  // Testing was already done at the unit test util level
  test('it renders', async function(assert) {
    this.set('inputValue', 'last_jedi');

    await render(hbs`{{nacho-core/titleize inputValue}}`);

    assert.equal(this.element.textContent?.trim(), 'Last Jedi');
  });
});
