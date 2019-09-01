import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | disable bubble input', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{disable-bubble-input id='test-disable'}}`);

    assert.equal(document.querySelector('#test-disable').tagName, 'INPUT');
  });

  test('it invokes the click action on click', async function(assert) {
    const inputClickAction = () => {
      assert.ok(true, 'disable input click action invoked');
    };
    this.set('inputClickAction', inputClickAction);

    await render(hbs`{{disable-bubble-input click=inputClickAction}}`);

    triggerEvent('input[type=text]', 'click');
  });
});
