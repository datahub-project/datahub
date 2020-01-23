import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, triggerKeyEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import Service from '@ember/service';
import { Keyboard } from 'wherehows-web/constants/keyboard';

module('Integration | Component | hotkeys/global-hotkeys', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{hotkeys/global-hotkeys}}`);
    assert.equal(this.element.textContent.trim(), '');
  });

  test('it detects target elligibility', async function(assert) {
    const HotKeyService = Service.extend({
      applyKeyMapping(keyCode) {
        assert.ok(true, 'Applied key mapping is called for key: ' + keyCode);
      }
    });

    this.owner.register('service:hot-keys', HotKeyService);
    assert.expect(2);

    await render(hbs`<div class="ember-application">
                       {{#hotkeys/global-hotkeys}}
                         <div id="pika-test"></div>
                       {{/hotkeys/global-hotkeys}}
                     </div>`);

    assert.ok(this.element, 'Still renders without errors');
    triggerKeyEvent('#pika-test', 'keyup', Keyboard.Slash);

    await render(hbs`<div class="ember-application">
                       {{#hotkeys/global-hotkeys}}
                         <input id="pika-test">
                       {{/hotkeys/global-hotkeys}}
                     </div>`);
    // This is expected to not call our apply method, hence only 2 assertions in this test
    triggerKeyEvent('#pika-test', 'keyup', Keyboard.Slash);
  });
});
