import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, fillIn, findAll, click, triggerKeyEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const basePillClass = '.nacho-pill';
const baseInputPillClass = '.nacho-pill-input';

module('Integration | Component | nacho-pill-input', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders and behaves as expected', async function(assert) {
    assert.expect(4);
    await render(hbs`{{nacho-pill-input}}`);
    assert.ok(this.element, 'Initial render is without errors');

    this.set('onDelete', () => {
      assert.ok(true, 'Delete was run successfully');
    });

    await render(hbs`{{nacho-pill-input
                       value="Pikachu"
                       onDelete=onDelete
                      }}`);

    assert.equal(this.element.textContent?.trim(), 'Pikachu', 'Renders the tag as expected');
    assert.equal(
      findAll(`${basePillClass}--neutral-inverse`).length,
      1,
      'Defaults to class as expected when there is a value'
    );

    await click(`${baseInputPillClass}__action`);
  });

  test('editing mode works and behaves as expected', async function(assert) {
    assert.expect(9);

    let counter = 1;
    this.set('onConfirm', (value: string) => {
      assert.equal(value, 'Charmander', '' + counter + ': confirm action success');
      counter++;
    });

    await render(hbs`{{nacho-pill-input
                       placeholder="Eevee"
                       onComplete=onConfirm
                     }}`);
    assert.equal(this.element.textContent?.trim(), 'Eevee', 'When there is no value, renders placeholder as expected');
    assert.equal(findAll('input').length, 0, 'Base case, no input by default');
    await click(`${baseInputPillClass}__action`);
    assert.equal(findAll('input').length, 1, 'Renders input by going into edit mode upon clicking action');

    /**
     * When "Enter" is used to complete input
     */
    await fillIn('input', 'Charmander');
    assert.equal(this.element.querySelector('input')?.value, 'Charmander', 'Input filled in as expected');
    // keyevent code for "Enter"
    await triggerKeyEvent('input', 'keyup', 13);
    // expect "onConfirm" to hit after enter is pressed

    /**
     * When Clicking the button that contains the icon is used to complte input
     */
    await render(hbs`{{nacho-pill-input
                       placeholder="Eevee"
                       onComplete=onConfirm
                     }}`);

    // Triggers editing mode and another onComplete action
    await click(`${baseInputPillClass}__action`);
    await fillIn('input', 'Charmander');
    assert.equal(this.element.querySelector('input')?.value, 'Charmander', 'Input filled in as expected');
    await click(`${baseInputPillClass}__action`);
    // expect "onConfirm" to hit after "onClick" is activated on button ( + icon )

    /**
     * When Tab is used to complete input
     */
    await render(hbs`{{nacho-pill-input
                    placeholder="Eevee"
                    onComplete=onConfirm
                  }}`);
    await click(`${baseInputPillClass}__action`);
    await fillIn('input', 'Charmander');
    //key event code for "Tab"
    assert.equal(this.element.querySelector('input')?.value, 'Charmander', 'Input filled in as expected');
    await triggerKeyEvent('input', 'keydown', 9);
    // expect "onConfirm" to hit after tab is pressed
  });
});
