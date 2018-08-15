import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerKeyEvent, fillIn } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { readDatasetOwnerTypesWithoutConsumer } from 'wherehows-web/utils/api/datasets/owners';

module('Integration | Component | abstracts/stop-propagation', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{abstracts/stop-propagation}}`);
    assert.equal(this.element.textContent.trim(), '');
    await render(hbs`
      {{#abstracts/stop-propagation}}
        template block text
      {{/abstracts/stop-propagation}}
    `);
    assert.equal(this.element.textContent.trim(), 'template block text');
  });

  test('it stops key up propagation', async function(assert) {
    const functionShouldCall = () => {
      assert.ok(true, 'This function was correctly called');
    };

    const functionShouldNotCall = () => {
      assert.ok(false, 'This assertion should never run');
    };

    this.setProperties({ functionShouldCall, functionShouldNotCall });

    assert.expect(2);
    await render(hbs`
      <div onkeyup={{functionShouldNotCall}}>
        {{#abstracts/stop-propagation as |funcKeyUp|}}
          <div onkeyup={{funcKeyUp}}>
            <input id="pika-test" onkeyup={{functionShouldCall}} />
          </div>
        {{/abstracts/stop-propagation}}
      </div>
    `);

    assert.ok(this.element, 'Still renders without errors');
    this.$('#pika-test').trigger('keyup');
  });
});
