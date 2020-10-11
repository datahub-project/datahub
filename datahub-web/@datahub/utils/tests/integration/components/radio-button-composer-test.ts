import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | radio-button-composer', function(hooks): void {
  setupRenderingTest(hooks);

  const disabledClass = 'paralyzed-pikachu';

  test('decorated properties behave as expected', async function(assert): Promise<void> {
    this.setProperties({
      disabledClass,
      value: 'electrify',
      mouseEnter() {
        assert.ok(true, 'Function got called on mouseEnter');
      }
    });
    await render(hbs`<RadioButtonComposer
                       @disabledClass={{this.disabledClass}}
                       @name="testA"
                       @groupValue="groupA"
                       @value={{this.value}}
                       @onMouseEnter={{this.mouseEnter}}
                    >
                      {{value}}
                    </RadioButtonComposer>`);

    // Ensures the function assert was called.
    assert.expect(5);
    assert.equal(this.element.textContent?.trim(), 'electrify', 'renders expected value');
    assert.equal(findAll(`.${disabledClass}`).length, 0, 'Does not disable when not supposed to');

    await triggerEvent('span', 'mouseenter');

    await render(hbs`<RadioButtonComposer
                       @disabledClass={{this.disabledClass}}
                       @name="testB"
                       @groupValue="groupB"
                       @value={{this.value}}
                       @onMouseEnter={{this.mouseEnter}}
                       @disabled={{true}} />`);

    assert.equal(findAll(`.${disabledClass}`).length, 1, 'Renders the disabled class');
    await triggerEvent('span', 'mouseenter');
  });
});
